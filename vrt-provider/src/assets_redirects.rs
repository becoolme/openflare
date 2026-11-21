//! Cloudflare `_redirects` for static assets (subset of the documented
//! syntax).
//!
//! The file lives at the assets root, is never served, and its rules
//! apply **before** asset resolution: a matching rule wins even when a
//! file exists at the incoming path. Supported today:
//!
//! - static rules `/old /new [code]` (302 default; 301/302/303/307/308);
//! - one trailing splat `/blog/* /posts/:splat`;
//! - segment placeholders `/movies/:title /media/:title`;
//! - `200` proxying to a relative destination (served from the same
//!   manifest, no recursion).
//!
//! Out of scope (Cloudflare also rejects or documents as unsupported):
//! query-parameter conditions, country/cookie conditions, domain-level
//! rules, fragments in `source`, more than one splat, rewrites with
//! codes other than 200.
//!
//! Parsing is lenient like Cloudflare: a line that does not follow the
//! grammar is ignored, never a hard failure. Per-file caps match the
//! documented 2,000 static + 100 dynamic / 1,000 characters per line.

pub const MAX_STATIC_RULES: usize = 2_000;
pub const MAX_DYNAMIC_RULES: usize = 100;
pub const MAX_LINE_BYTES: usize = 1_000;
/// Bound on the parsed rule file we are willing to read from the store.
pub const MAX_FILE_BYTES: usize = 512 * 1024;

/// Deployment-time validation for `/_redirects`: at request time an
/// oversized or non-UTF-8 file fails closed (503 for every asset
/// request), so reject it before the deployment is published.
pub fn validate_file(bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_FILE_BYTES {
        return Err("_redirects exceeds the 512 KiB cap".into());
    }
    std::str::from_utf8(bytes).map_err(|_| "_redirects is not valid UTF-8")?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleKind {
    Static,
    Dynamic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedirectRule {
    /// Source pattern, without a fragment.
    pub source: String,
    /// Destination template (`:splat` / `:name` references allowed).
    pub destination: String,
    pub status: u16,
    pub kind: RuleKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedirectAction {
    /// Emit a 3xx with this `Location`.
    Redirect { status: u16, location: String },
    /// `200` rule: serve this relative path from the same manifest.
    Proxy(String),
}

/// Parse one `_redirects` file. Invalid lines are skipped; static rules
/// come before dynamic rules (Cloudflare's documented processing order)
/// and file order is preserved within each class.
pub fn parse_redirects(text: &str) -> Vec<RedirectRule> {
    let mut static_rules = Vec::new();
    let mut dynamic_rules = Vec::new();
    for line in text.lines() {
        if line.len() > MAX_LINE_BYTES {
            continue;
        }
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let (Some(source), Some(destination)) = (parts.next(), parts.next()) else {
            continue;
        };
        let status = match parts.next() {
            None => 302,
            Some(code) => match code.parse::<u16>() {
                Ok(v @ (200 | 301 | 302 | 303 | 307 | 308)) => v,
                _ => continue,
            },
        };
        if parts.next().is_some() || !valid_source(source) {
            continue;
        }
        // `200` proxying is relative-only.
        if status == 200 && !destination.starts_with('/') {
            continue;
        }
        let source = source.split('#').next().unwrap_or(source).to_string();
        let kind = if source.contains('*') || source.contains(':') {
            RuleKind::Dynamic
        } else {
            RuleKind::Static
        };
        let rule = RedirectRule {
            source,
            destination: destination.to_string(),
            status,
            kind,
        };
        match kind {
            RuleKind::Static if static_rules.len() < MAX_STATIC_RULES => static_rules.push(rule),
            RuleKind::Dynamic if dynamic_rules.len() < MAX_DYNAMIC_RULES => {
                dynamic_rules.push(rule)
            }
            _ => {}
        }
    }
    static_rules.extend(dynamic_rules);
    static_rules
}

fn valid_source(source: &str) -> bool {
    if !source.starts_with('/') || source.len() > MAX_LINE_BYTES {
        return false;
    }
    // At most one splat, and only as a whole final segment (`/blog/*`).
    // `*` inside a segment (`/x/y*`) is not supported by this subset.
    if let Some(idx) = source.find('*') {
        if source[idx + 1..].contains('*') || !source.ends_with("/*") {
            return false;
        }
    }
    // Placeholders must be whole segments named `[A-Za-z0-9_]+`.
    for segment in source.split('/') {
        if let Some(name) = segment.strip_prefix(':') {
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            {
                return false;
            }
        }
    }
    true
}

/// Match `path` against the rules; the first matching rule wins.
///
/// `path` is the RAW request path (still percent-encoded) — Cloudflare
/// runs `_redirects` before decoding, and capture values are inserted
/// into `Location` verbatim, so encoded delimiters stay inside the
/// filename instead of becoming separators, a query or a fragment.
/// `query` is the raw query string without `?`.
pub fn match_redirect(
    rules: &[RedirectRule],
    path: &str,
    query: Option<&str>,
) -> Option<RedirectAction> {
    for rule in rules {
        let Some(captures) = match_rule(rule, path) else {
            continue;
        };
        let mut location = crate::assets::substitute_placeholders(&rule.destination, &captures);
        if rule.status == 200 {
            let target = location.split(['?', '#']).next().unwrap_or("").to_string();
            if target.starts_with('/') {
                return Some(RedirectAction::Proxy(target));
            }
            continue;
        }
        // Preserve the incoming query string unless the destination
        // already carries one (Cloudflare keeps it for rule matching
        // but forwards it on the redirect).
        if let Some(q) = query.filter(|q| !q.is_empty()) {
            if !location.contains('?') {
                location = match location.split_once('#') {
                    Some((base, frag)) => format!("{base}?{q}#{frag}"),
                    None => format!("{location}?{q}"),
                };
            }
        }
        if location.starts_with("//") && !rule.destination.starts_with("//") {
            // A capture that starts a new segment must never turn a
            // relative destination into a protocol-relative URL:
            // `/en//evil.com` + `/:splat` is `/evil.com`, not
            // `//evil.com`. An explicitly authored `//host/...`
            // destination is preserved (Cloudflare keeps scheme-less
            // external targets).
            location = format!("/{}", location.trim_start_matches('/'));
        }
        return Some(RedirectAction::Redirect {
            status: rule.status,
            location: percent_encode_url(&location),
        });
    }
    None
}

/// Percent-encode bytes that are not safe inside a URL (`new URL(...)`
/// parity): valid `%XX` escapes, reserved characters and visible ASCII
/// pass through; spaces, control bytes and non-ASCII become `%XX`.
fn percent_encode_url(url: &str) -> String {
    let bytes = url.as_bytes();
    let mut out = String::with_capacity(url.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' && i + 2 < bytes.len() && bytes[i + 1].is_ascii_hexdigit() && bytes[i + 2].is_ascii_hexdigit() {
            out.push_str(&url[i..i + 3]);
            i += 3;
            continue;
        }
        if (0x21..0x7f).contains(&b) && !matches!(b, b'"' | b'<' | b'>' | b'\\' | b'^' | b'`' | b'{' | b'|' | b'}') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
        i += 1;
    }
    out
}

/// Captures in substitution order: `(name, value)`. `splat` carries the
/// remainder of the path for `/x/*`.
fn match_rule(rule: &RedirectRule, path: &str) -> Option<Vec<(String, String)>> {
    if rule.kind == RuleKind::Static {
        return (path == rule.source).then(Vec::new);
    }
    let src: Vec<&str> = rule.source.split('/').collect();
    let req: Vec<&str> = path.split('/').collect();
    // Both start with an empty segment (leading '/'); an empty path was
    // normalized to "/" before this point.
    let mut captures = Vec::new();
    let mut i = 0;
    while i < src.len() {
        let s = src[i];
        if s == "*" {
            // Trailing splat: capture the rest of the request path.
            let rest = req.get(i..).map(|r| r.join("/")).unwrap_or_default();
            captures.push(("splat".to_string(), rest));
            return Some(captures);
        }
        let Some(r) = req.get(i) else {
            return None;
        };
        if let Some(name) = s.strip_prefix(':') {
            if r.is_empty() {
                return None;
            }
            captures.push((name.to_string(), (*r).to_string()));
        } else if *r != s {
            return None;
        }
        i += 1;
    }
    (req.len() == src.len()).then_some(captures)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(text: &str) -> Vec<RedirectRule> {
        parse_redirects(text)
    }

    fn location(text: &str, path: &str, query: Option<&str>) -> Option<(u16, String)> {
        match match_redirect(&rules(text), path, query) {
            Some(RedirectAction::Redirect { status, location }) => Some((status, location)),
            _ => None,
        }
    }

    #[test]
    fn static_redirects_default_to_302() {
        let text = "/home301 / 301\n/home302 / 302\n/old /new\n";
        assert_eq!(location(text, "/old", None), Some((302, "/new".to_string())));
        assert_eq!(location(text, "/home301", None), Some((301, "/".to_string())));
        assert_eq!(location(text, "/home302", None), Some((302, "/".to_string())));
        assert_eq!(location(text, "/missing", None), None);
    }

    #[test]
    fn splats_and_placeholders() {
        let text = "/blog/* https://blog.example.com/:splat\n/movies/:title /media/:title\n";
        assert_eq!(
            location(text, "/blog/a/b", None),
            Some((302, "https://blog.example.com/a/b".to_string()))
        );
        assert_eq!(
            location(text, "/movies/heat", None),
            Some((302, "/media/heat".to_string()))
        );
        assert_eq!(location(text, "/movies", None), None);
        assert_eq!(location(text, "/movies/a/b", None), None);
    }

    #[test]
    fn query_is_preserved_unless_destination_has_one() {
        let text = "/a /b\n/c /d?fixed=1\n/e /f#frag\n";
        assert_eq!(
            location(text, "/a", Some("x=1&y=2")),
            Some((302, "/b?x=1&y=2".to_string()))
        );
        assert_eq!(
            location(text, "/c", Some("x=1")),
            Some((302, "/d?fixed=1".to_string()))
        );
        assert_eq!(
            location(text, "/e", Some("x=1")),
            Some((302, "/f?x=1#frag".to_string()))
        );
    }

    #[test]
    fn proxy_200_is_relative_only() {
        let text = "/a /b 200\n/c https://x.example/ 200\n";
        assert_eq!(
            match_redirect(&rules(text), "/a", None),
            Some(RedirectAction::Proxy("/b".to_string()))
        );
        assert_eq!(match_redirect(&rules(text), "/c", None), None);
    }

    #[test]
    fn invalid_lines_are_ignored() {
        let text = concat!(
            "# comment\n",
            "\n",
            "relative /x 301\n",
            "/a /b 999\n",
            "/x/y* /z 302\n",
            "/with#frag /dest 301\n",
            "/ok /dest 308\n",
        );
        let rules = rules(text);
        assert_eq!(rules.len(), 2);
        // `/a /b/c 200` is a valid relative proxy, not a malformed line.
        assert_eq!(
            match_redirect(&parse_redirects("/a /b/c 200\n"), "/a", None),
            Some(RedirectAction::Proxy("/b/c".to_string()))
        );
        assert_eq!(
            location(text, "/ok", None),
            Some((308, "/dest".to_string()))
        );
        // `#` is stripped from the source, so the rule matches `/with`.
        assert_eq!(
            location(text, "/with", None),
            Some((301, "/dest".to_string()))
        );

    }

    #[test]
    fn captures_keep_their_url_encoding() {
        // `_redirects` matches the raw request path, so an encoded
        // separator can never turn a local rule into a cross-origin
        // Location (`//evil.com`) or leak into a query/fragment.
        let text = "/en/* /:splat 301\n/blog/* /posts/:splat\n";
        assert_eq!(
            location(text, "/en/%2Fevil.com", None),
            Some((301, "/%2Fevil.com".to_string()))
        );
        assert_eq!(
            location(text, "/en/%2E%2E%2Fadmin", None),
            Some((301, "/%2E%2E%2Fadmin".to_string()))
        );
        assert_eq!(
            location(text, "/blog/a%3Fb", Some("q=1")),
            Some((302, "/posts/a%3Fb?q=1".to_string()))
        );
        assert_eq!(
            location(text, "/blog/a%23b", None),
            Some((302, "/posts/a%23b".to_string()))
        );
        // A leading slash in a capture must stay origin-relative.
        assert_eq!(
            location(text, "/en//evil.com", None),
            Some((301, "/evil.com".to_string()))
        );
        // An explicitly authored protocol-relative destination is kept.
        assert_eq!(
            location("/cdn/* //cdn.example.com/:splat 301\n", "/cdn/app.js", None),
            Some((301, "//cdn.example.com/app.js".to_string()))
        );
        // Real segment boundaries are still preserved.
        assert_eq!(
            location(text, "/blog/a/b", None),
            Some((302, "/posts/a/b".to_string()))
        );
        // Authored non-ASCII destinations are percent-encoded like
        // `new URL(...)` would.
        assert_eq!(
            location("/cafe /café 301\n", "/cafe", None),
            Some((301, "/caf%C3%A9".to_string()))
        );
        // Existing escapes are never double-encoded.
        assert_eq!(
            location("/x /a%20b 301\n", "/x", None),
            Some((301, "/a%20b".to_string()))
        );
    }

    #[test]
    fn placeholders_substitute_in_one_pass() {
        // `:id` must not eat the prefix of `:identity`.
        let text = "/a/:id/:identity /b/:identity/:id\n";
        assert_eq!(
            location(text, "/a/12/34", None),
            Some((302, "/b/34/12".to_string()))
        );
        // A captured placeholder-like value is never rescanned.
        let text = "/a/:id/:other /b/:id/:other\n";
        assert_eq!(
            location(text, "/a/:other/x", None),
            Some((302, "/b/:other/x".to_string()))
        );
        // Proxy targets go through the same substitution.
        assert_eq!(
            match_redirect(&rules("/x/:id/:id2 /static/:id2/:id 200\n"), "/x/a/b", None),
            Some(RedirectAction::Proxy("/static/b/a".to_string()))
        );
        // Unknown placeholders stay literal.
        assert_eq!(
            location("/k/:id /d/:missing\n", "/k/1", None),
            Some((302, "/d/:missing".to_string()))
        );
    }

    #[test]
    fn static_rules_are_checked_before_dynamic_ones() {
        let text = "/b/* /dynamic\n/b/x /static\n";
        // Static rules are collected first, so /b/x wins even though the
        // dynamic rule appears higher in the file (Cloudflare's order).
        assert_eq!(
            location(text, "/b/x", None),
            Some((302, "/static".to_string()))
        );
        assert_eq!(
            location(text, "/b/y", None),
            Some((302, "/dynamic".to_string()))
        );
    }

    #[test]
    fn caps_are_enforced() {
        let mut text = String::new();
        for i in 0..=MAX_STATIC_RULES {
            text.push_str(&format!("/s{i} /t{i}\n"));
        }
        for i in 0..=MAX_DYNAMIC_RULES {
            text.push_str(&format!("/d{i}/* /t{i}\n"));
        }
        let rules = rules(&text);
        assert_eq!(
            rules.iter().filter(|r| r.kind == RuleKind::Static).count(),
            MAX_STATIC_RULES
        );
        assert_eq!(
            rules.iter().filter(|r| r.kind == RuleKind::Dynamic).count(),
            MAX_DYNAMIC_RULES
        );
    }
}
