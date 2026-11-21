//! Cloudflare Pages-style `_routes.json` invocation routes.
//!
//! `_routes.json` decides which paths run the user Worker and which are
//! served directly from static assets. Cloudflare documents it for
//! Pages Functions (Workers Static Assets uses `run_worker_first`
//! instead), but Pages projects routinely ship the file in their build
//! output, so the assets binding honours the same contract:
//!
//! - `version` is the only schema version, and must be `1`;
//! - `include` routes invoke the Worker, `exclude` routes are served by
//!   assets, and `exclude` always wins over `include`;
//! - a path matching neither list is served by assets (Pages default);
//! - `*` is a trailing wildcard matching any number of path segments
//!   (`/users/*`), so patterns are prefix matches; every other rule is
//!   an exact path.
//!
//! Limits (Cloudflare Pages): at least one `include` rule, no more than
//! 100 `include` + `exclude` rules combined, and at most 100 characters
//! per rule.
//!
//! Unlike `_headers`/`_redirects`, an invalid `_routes.json` is never
//! parsed leniently: routing decides whether the Worker runs at all, so
//! a malformed policy fails closed (the router answers a typed 503)
//! instead of silently routing requests to the wrong side.

/// Cloudflare Pages bound: at most 100 include+exclude rules.
pub const MAX_RULES: usize = 100;
/// Cloudflare Pages bound: at most 100 characters per route rule.
pub const MAX_RULE_CHARS: usize = 100;
/// Bound on the file read from the store. 100 rules x 100 characters
/// plus JSON overhead is far below this; anything larger is abusive.
pub const MAX_FILE_BYTES: usize = 64 * 1024;

/// Parsed `_routes.json` policy. Rules stay as authored (`/users/*`)
/// so the router wire can carry them unchanged and both sides match
/// with the same function.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RoutesConfig {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl RoutesConfig {
    /// Pages invocation decision for one raw request path: `true` when
    /// the path invokes the Worker. `exclude` has priority over
    /// `include`; a path matching neither list is served by assets.
    pub fn worker_first(&self, path: &str) -> bool {
        if self.exclude.iter().any(|rule| rule_matches(rule, path)) {
            return false;
        }
        self.include.iter().any(|rule| rule_matches(rule, path))
    }
}

/// Does one rule match `path`? `*` is a trailing, segment-agnostic
/// wildcard (prefix match); everything else is exact.
fn rule_matches(rule: &str, path: &str) -> bool {
    match rule.strip_suffix('*') {
        Some(prefix) => path.starts_with(prefix),
        None => path == rule,
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutesDoc {
    version: u32,
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
}

/// Parse and validate `_routes.json`. Every deviation is an error —
/// callers fail closed (503 for routed traffic, deploy-time rejection
/// for `vrt deploy`).
pub fn parse_routes(bytes: &[u8]) -> Result<RoutesConfig, String> {
    if bytes.len() > MAX_FILE_BYTES {
        return Err("_routes.json exceeds the 64 KiB cap".into());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| "_routes.json is not valid UTF-8")?;
    let doc: RoutesDoc =
        serde_json::from_str(text).map_err(|e| format!("_routes.json is not valid JSON: {e}"))?;
    if doc.version != 1 {
        return Err("_routes.json version must be 1".into());
    }
    if doc.include.is_empty() {
        return Err("_routes.json needs at least one include rule".into());
    }
    if doc.include.len() + doc.exclude.len() > MAX_RULES {
        return Err(format!(
            "_routes.json has more than {MAX_RULES} include/exclude rules"
        ));
    }
    for rule in doc.include.iter().chain(doc.exclude.iter()) {
        validate_rule(rule)?;
    }
    Ok(RoutesConfig {
        include: doc.include,
        exclude: doc.exclude,
    })
}

/// Deployment-time validation shared by `vrt-local`'s packer: a
/// `_routes.json` the provider would reject must never publish.
pub fn validate_file(bytes: &[u8]) -> Result<(), String> {
    parse_routes(bytes).map(|_| ())
}

fn validate_rule(rule: &str) -> Result<(), String> {
    if rule.is_empty() {
        return Err("_routes.json contains an empty rule".into());
    }
    if rule.chars().count() > MAX_RULE_CHARS {
        return Err(format!(
            "_routes.json rule exceeds {MAX_RULE_CHARS} characters"
        ));
    }
    if !rule.starts_with('/') {
        return Err("_routes.json rules must start with /".into());
    }
    if !rule.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        return Err("_routes.json rules must be printable ASCII".into());
    }
    // Query strings and fragments are never part of a request path, and
    // Pages/Workers route patterns only support the trailing wildcard.
    if rule.contains(['?', '#']) {
        return Err("_routes.json rules must be path-only".into());
    }
    if let Some(star) = rule.find('*') {
        if star + 1 != rule.len() {
            return Err("_routes.json only supports a trailing * wildcard".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cloudflare_examples() {
        let cfg = parse_routes(br#"{"version":1,"include":["/*"],"exclude":[]}"#).unwrap();
        assert!(cfg.worker_first("/anything"));
        let cfg =
            parse_routes(br#"{"version":1,"include":["/*"],"exclude":["/build/*"]}"#).unwrap();
        assert!(cfg.worker_first("/app.js"));
        assert!(!cfg.worker_first("/build/app.js"));
        assert!(!cfg.worker_first("/build/"));
    }

    #[test]
    fn exclude_wins_over_include_and_default_is_assets() {
        let cfg = parse_routes(
            br#"{"version":1,"include":["/api/*","/exact"],"exclude":["/api/public/*","/exact2"]}"#,
        )
        .unwrap();
        assert!(cfg.worker_first("/api/users"));
        assert!(cfg.worker_first("/api/"));
        assert!(!cfg.worker_first("/api/public/data.txt"));
        assert!(cfg.worker_first("/exact"));
        assert!(!cfg.worker_first("/exact2"));
        // Matches neither list -> assets.
        assert!(!cfg.worker_first("/index.html"));
        // Exact rules do not prefix-match.
        assert!(!cfg.worker_first("/exact/more"));
        // A missing exclude list is allowed.
        let cfg = parse_routes(br#"{"version":1,"include":["/*"]}"#).unwrap();
        assert!(cfg.worker_first("/"));
    }

    #[test]
    fn wildcard_is_trailing_only_and_flags_malformed_rules() {
        for bad in [
            r#"{"version":1,"include":["/api/*/x"]}"#,
            r#"{"version":1,"include":["/*/api"]}"#,
            r#"{"version":1,"include":["/api/?x=1"]}"#,
            r#"{"version":1,"include":["/api/#f"]}"#,
            r#"{"version":1,"include":["api/*"]}"#,
            r#"{"version":1,"include":[""]}"#,
            r#"{"version":1,"include":["/a\tb"]}"#,
        ] {
            assert!(parse_routes(bad.as_bytes()).is_err(), "{bad}");
        }
    }

    #[test]
    fn enforces_version_include_and_limits() {
        assert!(parse_routes(br#"{"version":2,"include":["/*"]}"#).is_err());
        assert!(parse_routes(br#"{"version":1,"include":[]}"#).is_err());
        assert!(parse_routes(br#"{"version":1}"#).is_err()); // missing include
        assert!(parse_routes(br#"{"version":1,"include":["/*"],"extra":1}"#).is_err());

        let rules: Vec<String> = (0..MAX_RULES - 1).map(|i| format!("\"/e{i}/x\"")).collect();
        let doc = format!(
            r#"{{"version":1,"include":["/a"],"exclude":[{}]}}"#,
            rules.join(",")
        );
        let parsed = parse_routes(doc.as_bytes()).unwrap();
        assert_eq!(parsed.include.len(), 1);
        assert_eq!(parsed.exclude.len(), MAX_RULES - 1);
        // One more rule crosses the combined bound.
        let doc = format!(
            r#"{{"version":1,"include":["/a"],"exclude":[{},{}]}}"#,
            rules.join(","),
            r#""/over/x""#
        );
        assert!(parse_routes(doc.as_bytes()).is_err());
        assert!(parse_routes(
            format!(
                "{{\"version\":1,\"include\":[\"/{}\"]}}",
                "a".repeat(MAX_RULE_CHARS)
            )
            .as_bytes()
        )
        .is_err());
        assert!(parse_routes(
            format!(
                "{{\"version\":1,\"include\":[\"/{}\"]}}",
                "a".repeat(MAX_RULE_CHARS - 1)
            )
            .as_bytes()
        )
        .is_ok());
        let mut huge = String::from(r#"{"version":1,"include":["/"#);
        while huge.len() <= MAX_FILE_BYTES {
            huge.push('a');
        }
        huge.push_str(r#""]}"#);
        assert!(parse_routes(huge.as_bytes()).is_err());
    }

    #[test]
    fn rejects_non_utf8_and_non_json() {
        assert!(parse_routes(b"{\"version\":1,\"include\":[\"/\xff\"]}").is_err());
        assert!(parse_routes(b"not json").is_err());
        assert!(validate_file(br#"{"version":1,"include":["/*"]}"#).is_ok());
    }
}
