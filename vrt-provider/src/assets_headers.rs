//! Cloudflare `_headers` for static assets (documented subset).
//!
//! The file lives at the assets root, is never served, and its rules are
//! applied to **static asset responses** in file order:
//!
//! - a rule matches a request path (`*` splat, `:name` placeholders,
//!   absolute `https://` URLs are reduced to their path);
//! - every matching rule applies (accumulating), later values for the
//!   same header are joined with `, `;
//! - `! Name` detaches values added by earlier rules;
//! - `:splat` / `:name` are substituted in header values.
//!
//! Not supported: query/country/cookie conditions (Cloudflare does not
//! support them either), more than one splat, and setting headers on
//! Worker-generated responses (only asset responses, like Cloudflare).
//!
//! Limits: 100 rules, 2,000 characters per line.

pub const MAX_RULES: usize = 100;
pub const MAX_LINE_BYTES: usize = 2_000;
/// Bound on the parsed file read from the store.
pub const MAX_FILE_BYTES: usize = 512 * 1024;
/// Bound on the names a response may advertise as `_headers`-touched —
/// the facade rejects a longer list wholesale (`MAX_CUSTOM_HEADER_NAMES`
/// in `assets.rs`), which would silently drop every configured header.
pub const MAX_FORWARDED_NAMES: usize = 256;
/// Byte bound on the joined name list (same forward contract).
pub const MAX_FORWARDED_NAME_BYTES: usize = 4096;

/// Deployment-time validation for `/_headers`.
///
/// At request time `header_rules` fails closed on an unreadable file
/// (503 for every asset request), and the facade drops the whole
/// forwarding list when its bound is exceeded. Both make an accepted
/// deployment unusable, so the packer rejects such a file before it is
/// published: size, UTF-8 and the forwarding bound are enforced here.
pub fn validate_file(bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_FILE_BYTES {
        return Err("_headers exceeds the 512 KiB cap".into());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| "_headers is not valid UTF-8")?;
    let rules = parse_headers(text);
    let mut names: Vec<&str> = Vec::new();
    for rule in &rules {
        for op in &rule.ops {
            let name = match op {
                HeaderOp::Set { name, .. } | HeaderOp::Detach { name } => name.as_str(),
            };
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }
    if names.len() > MAX_FORWARDED_NAMES {
        return Err("_headers touches too many header names to forward".into());
    }
    let joined: usize = names.iter().map(|n| n.len() + 1).sum();
    if joined > MAX_FORWARDED_NAME_BYTES {
        return Err("_headers header names exceed the forwarding bound".into());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderOp {
    Set { name: String, value: String },
    Detach { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderRule {
    /// Path pattern (`/static/*`, `/movies/:title`, ...).
    pub source: String,
    pub ops: Vec<HeaderOp>,
}

/// Parse a `_headers` file. Malformed lines/blocks are ignored (the
/// documented behaviour: the file is parsed leniently).
pub fn parse_headers(text: &str) -> Vec<HeaderRule> {
    let mut rules: Vec<HeaderRule> = Vec::new();
    let mut current: Option<HeaderRule> = None;
    for line in text.lines() {
        let indented = line.starts_with(' ') || line.starts_with('\t');
        if line.len() > MAX_LINE_BYTES {
            // An overlong *block opener* is an invalid source: close the
            // preceding block so its header ops cannot attach to it.
            // (Overlong indented lines are simply not an op.)
            if !indented {
                if let Some(rule) = current.take() {
                    if !rule.ops.is_empty() && rules.len() < MAX_RULES {
                        rules.push(rule);
                    }
                }
            }
            continue;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if !indented {
            if let Some(rule) = current.take() {
                if !rule.ops.is_empty() && rules.len() < MAX_RULES {
                    rules.push(rule);
                }
            }
            if let Some(source) = normalize_source(trimmed) {
                current = Some(HeaderRule {
                    source,
                    ops: Vec::new(),
                });
            }
            continue;
        }
        let Some(rule) = current.as_mut() else {
            continue;
        };
        if let Some(op) = parse_op(trimmed) {
            rule.ops.push(op);
        }
    }
    if let Some(rule) = current.take() {
        if !rule.ops.is_empty() && rules.len() < MAX_RULES {
            rules.push(rule);
        }
    }
    rules
}

/// Absolute URLs are reduced to their path; matching ignores protocol
/// and port (Cloudflare semantics).
fn normalize_source(raw: &str) -> Option<String> {
    let path = if let Some(rest) = raw
        .strip_prefix("https://")
        .or_else(|| raw.strip_prefix("http://"))
    {
        match rest.find('/') {
            Some(i) => &rest[i..],
            None => "/",
        }
    } else {
        raw
    };
    if !path.starts_with('/') {
        return None;
    }
    // One splat, anywhere (Cloudflare: at most one).
    if path.matches('*').count() > 1 {
        return None;
    }
    Some(path.to_string())
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~')
        })
}

fn forbidden(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("x-open-compute-")
        || name.starts_with("x-vrt-")
        || matches!(
            name.as_str(),
            "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
                | "content-length"
                | "host"
        )
}

fn parse_op(line: &str) -> Option<HeaderOp> {
    if let Some(rest) = line.strip_prefix('!') {
        let name = rest.trim();
        if valid_name(name) && !forbidden(name) {
            return Some(HeaderOp::Detach {
                name: name.to_ascii_lowercase(),
            });
        }
        return None;
    }
    let (name, value) = line.split_once(':')?;
    let name = name.trim();
    let value = value.trim();
    if !valid_name(name)
        || forbidden(name)
        || value.is_empty()
        || value.len() > MAX_LINE_BYTES
        || value.chars().any(|c| c.is_control())
    {
        return None;
    }
    Some(HeaderOp::Set {
        name: name.to_ascii_lowercase(),
        value: value.to_string(),
    })
}

/// Apply every matching rule, in file order, to `headers`.
///
/// Returns the header names touched (set or detached). The facade only
/// forwards those names, so arbitrary provider headers can never leak
/// into a tenant response.
pub fn apply_headers(
    rules: &[HeaderRule],
    path: &str,
    headers: &mut axum::http::HeaderMap,
) -> Vec<String> {
    let mut touched: Vec<String> = Vec::new();
    for rule in rules {
        let Some(captures) = match_pattern(&rule.source, path) else {
            continue;
        };
        for op in &rule.ops {
            match op {
                HeaderOp::Detach { name } => {
                    headers.remove(name.as_str());
                    if !touched.contains(name) {
                        touched.push(name.clone());
                    }
                }
                HeaderOp::Set { name, value } => {
                    let value =
                        crate::assets::substitute_placeholders(value, &captures);
                    let Ok(header_name) = axum::http::HeaderName::from_bytes(name.as_bytes()) else {
                        continue;
                    };
                    // First `_headers` write replaces the default value
                    // ("headers defined in `_headers` override what
                    // Cloudflare ordinarily sends"); later writes join.
                    let already = touched.contains(name);
                    if name == "set-cookie" {
                        if let Ok(v) = axum::http::HeaderValue::from_str(&value) {
                            headers.append(header_name, v);
                        }
                        if !already {
                            touched.push(name.clone());
                        }
                        continue;
                    }
                    // Join as raw bytes: an accepted non-ASCII value
                    // must survive accumulation (`to_str` rejects it and
                    // would silently replace the earlier value).
                    let joined: Vec<u8> = if already {
                        match headers.get(header_name.clone()) {
                            Some(existing) => {
                                let mut buf = existing.as_bytes().to_vec();
                                buf.extend_from_slice(b", ");
                                buf.extend_from_slice(value.as_bytes());
                                buf
                            }
                            None => value.into_bytes(),
                        }
                    } else {
                        value.into_bytes()
                    };
                    if let Ok(v) = axum::http::HeaderValue::from_bytes(&joined) {
                        headers.insert(header_name, v);
                    }
                    if !already {
                        touched.push(name.clone());
                    }
                }
            }
        }
    }
    touched
}

/// One pattern match; returns `(:name|splat, captured value)` pairs.
///
/// Mirrors Cloudflare's `generateRuleRegExp` (`rules-engine.ts`): `*`
/// becomes `.*` (one splat, any depth) and `:name` becomes `[^/]+`, so
/// placeholders and a splat can be combined (`/users/:id/*`).
fn match_pattern(source: &str, path: &str) -> Option<Vec<(String, String)>> {
    if let Some(star) = source.find('*') {
        let head = &source[..star];
        let tail = &source[star + 1..];
        let mut captures = Vec::new();
        let consumed;
        if head.ends_with('/') {
            // Segment-aligned head (`/`, `/static/`, `/users/:id/`):
            // match whole segments, capturing placeholders as we go.
            if !path.starts_with('/') {
                return None;
            }
            let segments: Vec<&str> = head[1..].split('/').filter(|s| !s.is_empty()).collect();
            let mut rest = &path[1..];
            for segment in segments {
                let end = rest.find('/')?;
                let value = &rest[..end];
                if value.is_empty() {
                    return None;
                }
                match segment.strip_prefix(':') {
                    Some(name) => captures.push((name.to_string(), value.to_string())),
                    None if segment == value => {}
                    None => return None,
                }
                rest = &rest[end + 1..];
            }
            consumed = path.len() - rest.len();
        } else {
            // Placeholder-free head, e.g. `/foo*bar.jpg`.
            if !path.starts_with(head) {
                return None;
            }
            consumed = head.len();
        }
        if path.len() < consumed + tail.len() || !path[consumed..].ends_with(tail) {
            return None;
        }
        let splat = &path[consumed..path.len() - tail.len()];
        captures.push(("splat".to_string(), splat.to_string()));
        return Some(captures);
    }
    let src: Vec<&str> = source.split('/').collect();
    let req: Vec<&str> = path.split('/').collect();
    if src.len() != req.len() {
        return None;
    }
    let mut captures = Vec::new();
    for (s, r) in src.iter().zip(req.iter()) {
        if let Some(name) = s.strip_prefix(':') {
            if r.is_empty() {
                return None;
            }
            captures.push((name.to_string(), (*r).to_string()));
        } else if s != r {
            return None;
        }
    }
    Some(captures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    fn headers(text: &str, path: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("content-type", HeaderValue::from_static("text/html"));
        h.insert("cache-control", HeaderValue::from_static("public, max-age=0"));
        let _ = apply_headers(&parse_headers(text), path, &mut h);
        h
    }

    #[test]
    fn rules_apply_in_order_and_accumulate() {
        let text = concat!(
            "/*\n",
            "  X-Frame-Options: DENY\n",
            "  X-Robots-Tag: noindex\n",
            "\n",
            "/static/*\n",
            "  Access-Control-Allow-Origin: *\n",
            "  X-Robots-Tag: nosnippet\n",
        );
        let h = headers(text, "/static/app.js");
        assert_eq!(h.get("x-frame-options").unwrap(), "DENY");
        assert_eq!(h.get("access-control-allow-origin").unwrap(), "*");
        assert_eq!(h.get("x-robots-tag").unwrap(), "noindex, nosnippet");
        // Non-matching narrower rule does not apply.
        let h = headers(text, "/other.txt");
        assert_eq!(h.get("x-frame-options").unwrap(), "DENY");
        assert!(h.get("access-control-allow-origin").is_none());
    }

    #[test]
    fn detach_removes_earlier_values_and_can_be_replaced() {
        let text = concat!(
            "/assets/*\n",
            "  Cache-Control: public, max-age=0, s-maxage=86400\n",
            "\n",
            "/assets/model.bin\n",
            "  ! Cache-Control\n",
            "  Cache-Control: public, max-age=604800\n",
        );
        let h = headers(text, "/assets/model.bin");
        assert_eq!(h.get("cache-control").unwrap(), "public, max-age=604800");
        let h = headers(text, "/assets/other.bin");
        assert_eq!(
            h.get("cache-control").unwrap(),
            "public, max-age=0, s-maxage=86400"
        );
    }

    #[test]
    fn splat_and_placeholder_substitution() {
        let text = concat!(
            "/*.jpg\n",
            "  X-Splat: \":splat\"\n",
            "\n",
            "/movies/:title\n",
            "  X-Movie: 'You are watching :title'\n",
            "\n",
            "https://example.com/abs/*\n",
            "  X-Abs: \":splat\"\n",
        );
        let h = headers(text, "/a/b/pic.jpg");
        assert_eq!(h.get("x-splat").unwrap(), "\"a/b/pic\"");
        let h = headers(text, "/movies/heat");
        assert_eq!(h.get("x-movie").unwrap(), "'You are watching heat'");
        let h = headers(text, "/abs/x/y");
        assert_eq!(h.get("x-abs").unwrap(), "\"x/y\"");
    }

    #[test]
    fn dangerous_and_internal_headers_are_ignored() {
        let text = concat!(
            "/*\n",
            "  X-Open-Compute-Worker-Id: forged\n",
            "  X-Vrt-Tenant-Id: forged\n",
            "  Connection: close\n",
            "  Content-Length: 9999\n",
            "  X-Ok: yes\n",
        );
        let h = headers(text, "/x");
        assert!(h.get("x-open-compute-worker-id").is_none());
        assert!(h.get("x-vrt-tenant-id").is_none());
        assert!(h.get("connection").is_none());
        // The provider's own content-length is untouched.
        assert!(h.get("content-length").is_none());
        assert_eq!(h.get("x-ok").unwrap(), "yes");
    }

    #[test]
    fn overlong_block_opener_closes_the_previous_block() {
        // A path longer than a `_headers` line must not let its header
        // ops attach to the preceding rule.
        let long = format!("/{}", "a".repeat(MAX_LINE_BYTES + 10));
        let text = format!("/public/*\n  X-Frame-Options: DENY\n{long}\n  Access-Control-Allow-Origin: *\n");
        let rules = parse_headers(&text);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].source, "/public/*");
        assert_eq!(rules[0].ops.len(), 1);
        let h = headers(&text, "/public/x");
        assert!(h.get("access-control-allow-origin").is_none());
    }

    #[test]
    fn placeholders_and_a_splat_combine() {
        let text = concat!(
            "/users/:id/*\n",
            "  X-User: :id\n",
            "  X-Path: :splat\n",
            "\n",
            "/*.jpg\n",
            "  X-Splat: :splat\n",
        );
        let h = headers(text, "/users/42/avatar.png");
        assert_eq!(h.get("x-user").unwrap(), "42");
        assert_eq!(h.get("x-path").unwrap(), "avatar.png");
        // Suffix splats keep their previous semantics.
        let h = headers(text, "/a/b/pic.jpg");
        assert_eq!(h.get("x-splat").unwrap(), "a/b/pic");
        // The placeholder still constrains the segment.
        let h = headers(text, "/users");
        assert!(h.get("x-user").is_none());
    }

    #[test]
    fn placeholder_names_substitute_in_one_pass() {
        let text = "/items/:id/:id2\n  X-Item: :id2\n  X-Pair: :id-:id2\n";
        let h = headers(text, "/items/a/b");
        assert_eq!(h.get("x-item").unwrap(), "b");
        assert_eq!(h.get("x-pair").unwrap(), "a-b");
        // A captured placeholder-like value is not expanded again.
        let text = "/a/:id\n  X-Raw: :id\n";
        let h = headers(text, "/a/:other");
        assert_eq!(h.get("x-raw").unwrap(), ":other");
    }

    #[test]
    fn repeated_non_ascii_values_are_kept() {
        let text = "/*\n  X-Label: café\n\n/x\n  X-Label: second\n";
        let h = headers(text, "/x");
        assert_eq!(h.get("x-label").unwrap().as_bytes(), "café, second".as_bytes());
    }

    #[test]
    fn deployment_validation_rejects_unusable_files() {
        // Valid files pass.
        assert!(validate_file(b"/*\n  X-Frame-Options: DENY\n").is_ok());
        assert!(validate_file(b"# only comments\n").is_ok());
        // Non-UTF-8 and oversized files would 503 every asset request.
        assert!(validate_file(b"/*\n  X-A: \xff\xfe\n").is_err());
        let mut big = String::new();
        while big.len() <= MAX_FILE_BYTES {
            big.push_str("# padding padding padding padding padding\n");
        }
        assert!(validate_file(big.as_bytes()).is_err());
        // Too many / too long forwarded names would drop every header.
        // A single accepted block can carry them all.
        let mut many = String::from("/*\n");
        for i in 0..=MAX_FORWARDED_NAMES {
            many.push_str(&format!("  X-H{i}: v\n"));
        }
        assert!(validate_file(many.as_bytes()).is_err());
        let mut long = String::new();
        for i in 0..40 {
            long.push_str(&format!("/r{i}\n  X-{}-{i}: v\n", "a".repeat(120)));
        }
        assert!(validate_file(long.as_bytes()).is_err());
    }

    #[test]
    fn invalid_blocks_are_ignored_and_rules_are_capped() {
        assert!(parse_headers("not-a-path\n  X-A: 1\n").is_empty());
        assert!(parse_headers("/x\n  no-colon\n").is_empty());
        let mut text = String::new();
        for i in 0..=MAX_RULES {
            text.push_str(&format!("/r{i}\n  X-N: {i}\n"));
        }
        assert_eq!(parse_headers(&text).len(), MAX_RULES);
    }
}
