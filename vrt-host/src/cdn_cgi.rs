//! `/cdn-cgi/image/<options>/<src>` request-path transforms (IMG1).
//!
//! Cloudflare reserves the `/cdn-cgi/` prefix on every proxied zone for
//! internal features; `image` runs the same transform vocabulary the
//! `env.IMAGES` binding exposes, fetched + resized at the edge. Here the
//! ingress intercepts the path BEFORE the tenant worker, mirrors CF's
//! flow: fetch the source through the tenant's own socket (same-origin
//! only — absolute URLs must name the request Host, so the endpoint can
//! never be turned into an SSRF oracle), then POST a multipart body to
//! the per-worker `facade-images` socket (`<workerId>-images.sock`,
//! emitted by capnp when the worker declares an `images` binding).
//!
//! Gating: a tenant without an `images` binding keeps `/cdn-cgi/` as an
//! ordinary worker path (conservative — CF intercepts always, but a
//! silent 4xx on tenants that never opted in would be a behavior fork).
//! Errors are rendered in CF's own shape: `ERROR <code>` text bodies.

use crate::endpoint::{UpstreamIo, WorkerEndpoint};
use crate::ingress::IngressError;
use crate::route::RouteTarget;
use crate::supervisor::TenantRuntime;
use axum::body::Body;
use axum::body::Bytes;
use axum::http::{header, HeaderMap, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use std::sync::Arc;

const PREFIX: &str = "/cdn-cgi/image/";
/// Source-image bound — mirrors the binding's 64MiB body cap.
const MAX_SRC_BYTES: usize = 64 * 1024 * 1024;
const UPSTREAM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn error_body(code: u32, status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from(format!("ERROR {code}\n")))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

/// `Some(resp)` when the request is a `/cdn-cgi/image/` transform the
/// host owns; `None` falls through to the tenant worker untouched.
pub async fn maybe_handle(
    method: &axum::http::Method,
    path: &str,
    headers: &HeaderMap,
    host: &str,
    target: &RouteTarget,
    runtime: &Arc<TenantRuntime>,
) -> Option<Result<Response<Body>, IngressError>> {
    if !target.images {
        return None;
    }
    let rest = match path.strip_prefix(PREFIX) {
        Some(r) => r,
        None => return None,
    };
    if *method != axum::http::Method::GET && *method != axum::http::Method::HEAD {
        return Some(Ok(error_body(9406, StatusCode::METHOD_NOT_ALLOWED)));
    }
    let (opts_raw, src) = match rest.split_once('/') {
        Some((o, s)) if !o.is_empty() => (o, s),
        _ => return Some(Ok(error_body(9423, StatusCode::BAD_REQUEST))),
    };
    // Same-origin only: `/path` is relative to the tenant ingress;
    // `http(s)://<request-host>/...` is rewritten to its path. Any other
    // absolute host is refused — there is no remote-fetch allowlist yet.
    let src_path = if src.starts_with('/') || !src.contains("://") {
        format!("/{src}")
    } else {
        let bad_src = || Some(Ok(error_body(9423, StatusCode::BAD_REQUEST)));
        let Some((scheme, tail)) = src.split_once("://") else {
            return bad_src();
        };
        if scheme != "http" && scheme != "https" {
            return bad_src();
        }
        let Some((url_host, url_path)) = tail.split_once('/') else {
            return bad_src();
        };
        let url_host = url_host.split(':').next().unwrap_or_default();
        if !url_host.eq_ignore_ascii_case(host) {
            return Some(Ok(error_body(9401, StatusCode::FORBIDDEN)));
        }
        format!("/{url_path}")
    };
    let parsed = match parse_options(opts_raw, headers, host) {
        Ok(p) => p,
        Err(code) => return Some(Ok(error_body(code, StatusCode::BAD_REQUEST))),
    };
    Some(run_transform(headers, &src_path, parsed, target, runtime, host).await)
}

struct Parsed {
    transforms: String,
    output_format: Option<String>,
    output_quality: Option<u32>,
    anim: bool,
    /// `metadata=exif` routes to `/info` (platform extension — the
    /// upstream URL vocabulary has no source-info verb of its own).
    info: bool,
    /// `metadata=none|copyright|keep` — sent on the transform wire.
    wire_metadata: Option<String>,
    onerror_redirect: bool,
    /// `draw=((...),(...))` overlay entries — each fetched same-origin
    /// and posted as a `draw_image` field (`frames[1..]` on the wire).
    draws: Vec<DrawEntry>,
}

struct DrawEntry {
    /// Resolved same-origin overlay path.
    path: String,
    /// Position/blend keys → `{drawImageIndex, targetImageIndex, ...}`
    /// op; indices are stamped at emit time.
    draw_op: serde_json::Map<String, serde_json::Value>,
    /// Overlay optimization params → `{imageIndex, percentBasis, ...}`
    /// op (skipped when empty).
    transform: serde_json::Map<String, serde_json::Value>,
}

/// `quality` value: fixed 1-100 or a perceptual tier. Upstream
/// publishes no integer mapping; the anchors match what other
/// CF-compat implementations picked.
fn quality_level(s: &str) -> Result<u32, u32> {
    match s {
        "high" => Ok(90),
        "medium-high" => Ok(80),
        "medium-low" => Ok(60),
        "low" => Ok(40),
        _ => Ok(parse_num(s, 1.0, 100.0)? as u32),
    }
}

/// `%XX` escapes decode once per option value — upstream URLs carry
/// percent-encoded `#`/`(` characters in e.g. color args. Malformed
/// sequences pass through so the option's own validation types them.
fn pct_decode(s: &str) -> String {
    fn hex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    if !s.contains('%') {
        return s.to_string();
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let dec = if bytes[i] == b'%' && i + 2 < bytes.len() {
            match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                (Some(h), Some(l)) => Some(h * 16 + l),
                _ => None,
            }
        } else {
            None
        };
        if let Some(d) = dec {
            out.push(d);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_num(s: &str, min: f64, max: f64) -> Result<f64, u32> {
    let v: f64 = s.parse().map_err(|_| 9423u32)?;
    if !v.is_finite() || v < min || v > max {
        return Err(9423);
    }
    Ok(v)
}

/// Top-level option split: commas inside `(...)` groups do not split —
/// the structured form nests `k=v` pairs (and `(a,b)` lists) under one
/// parameter, e.g. `sharpen=(mode=gaussian,amount=1,sigma=1.5)`.
fn split_opts(raw: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, c) in raw.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&raw[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&raw[start..]);
    out
}

/// `(k=v,...)` group → its inner comma-list, else `None`.
fn group_inner(v: &str) -> Option<&str> {
    let v = v.trim();
    v.strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .map(str::trim)
}

fn group_pairs(inner: &str) -> Result<Vec<(&str, &str)>, u32> {
    split_opts(inner)
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .map(|pair| pair.split_once('=').ok_or(9423u32))
        .collect()
}

/// Comma split that also treats `'...'`/`"..."` quoting as opaque —
/// `draw` entries carry quoted URLs/text where commas and parens are
/// literal content, unlike the top-level option grammar.
fn split_draw(raw: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut start = 0usize;
    for (i, c) in raw.char_indices() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    out.push(&raw[start..i]);
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    out.push(&raw[start..]);
    out
}

/// One layer of matching `'...'`/`"..."` quotes off a value.
fn unquote(s: &str) -> &str {
    let s = s.trim();
    if s.len() >= 2 {
        let (f, l) = (s.as_bytes()[0], s.as_bytes()[s.len() - 1]);
        if (f == b'\'' || f == b'"') && l == f {
            return &s[1..s.len() - 1];
        }
    }
    s
}

/// Scalar leaf → JSON: unquoted, then number/bool/string by shape.
fn leaf_json(v: &str) -> serde_json::Value {
    let v = unquote(v);
    if let Ok(n) = v.parse::<f64>() {
        if n.is_finite() {
            return n.into();
        }
    }
    match v {
        "true" => true.into(),
        "false" => false.into(),
        _ => v.into(),
    }
}

/// `(k=v,...)` group → a JSON object — the generic form for draw-entry
/// structured values (`sharpen=(..)`, `border=(..)`); leaf values type
/// by shape and nested groups recurse.
fn group_to_json(inner: &str) -> Result<serde_json::Value, u32> {
    let mut m = serde_json::Map::new();
    for pair in split_draw(inner) {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').ok_or(9423u32)?;
        let v = v.trim();
        let val = match group_inner(v) {
            Some(nested) => group_to_json(nested)?,
            None => leaf_json(v),
        };
        m.insert(k.trim().to_string(), val);
    }
    Ok(serde_json::Value::Object(m))
}

/// Chromium client hints for `quality.slow-connection` (upstream
/// applies it when rtt>150ms, save-data=on, ect is 2g-class, or
/// downlink<5Mbps).
fn slow_connection(headers: &HeaderMap) -> bool {
    let h = |k: &str| headers.get(k).and_then(|v| v.to_str().ok());
    if h("rtt")
        .and_then(|v| v.parse::<f64>().ok())
        .is_some_and(|r| r > 150.0)
    {
        return true;
    }
    if h("save-data").is_some_and(|v| v.eq_ignore_ascii_case("on")) {
        return true;
    }
    if h("ect").is_some_and(|v| matches!(v, "slow-2g" | "2g" | "3g")) {
        return true;
    }
    if h("downlink")
        .and_then(|v| v.parse::<f64>().ok())
        .is_some_and(|d| d < 5.0)
    {
        return true;
    }
    false
}

/// `width=auto`: Viewport-Width/Sec-CH-Viewport-Width picks the
/// smallest breakpoint >= the viewport (largest when beyond all);
/// otherwise a UA heuristic picks mobile/desktop fallbacks — the same
/// two upstream fallback sources, with upstream's defaults.
fn auto_width(headers: &HeaderMap, breakpoints: &[f64], mobile: f64, desktop: f64) -> f64 {
    let h = |k: &str| headers.get(k).and_then(|v| v.to_str().ok());
    let vw = h("viewport-width")
        .or_else(|| h("sec-ch-viewport-width"))
        .and_then(|v| v.parse::<f64>().ok());
    if let Some(vw) = vw {
        if vw > 0.0 {
            let mut bps: Vec<f64> = breakpoints.to_vec();
            bps.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            return bps
                .iter()
                .copied()
                .find(|&b| b >= vw)
                .unwrap_or_else(|| *bps.last().unwrap_or(&desktop));
        }
    }
    let ua = h("user-agent").unwrap_or_default();
    if ua.contains("Mobile")
        || ua.contains("Android")
        || ua.contains("iPhone")
        || ua.contains("iPad")
    {
        mobile
    } else {
        desktop
    }
}

/// A trim/border length on the URL: `"Np"` percent strings pass
/// through for the facade to resolve against the axis length;
/// anything else is a nonnegative finite number.
fn trim_len(s: &str) -> Result<serde_json::Value, u32> {
    if let Some(p) = s.strip_suffix('p') {
        parse_num(p, 0.0, 100.0)?;
        return Ok(s.into());
    }
    Ok(parse_num(s, 0.0, 16384.0)?.into())
}

/// Multiply whole-pixel values (>=1) of the named keys by dpr —
/// fractions (0..1) and `"Np"` percent strings pass through for the
/// facade to resolve against the real axis length.
fn scale_px(v: &mut serde_json::Value, dpr: f64) {
    for key in ["top", "right", "bottom", "left", "width", "height", "keep"] {
        if let Some(n) = v.get(key).and_then(|x| x.as_f64()) {
            if n >= 1.0 {
                v[key] = (n * dpr).round().into();
            }
        }
    }
}

/// Overlay `url=` → its same-origin path, mirroring the source-URL
/// resolution in `maybe_handle`: `/path` and bare relative segments
/// ride the tenant origin; `http(s)://<request-host>/...` rewrites to
/// its path. Any other absolute host is refused — draw entries have
/// no remote-fetch allowlist either.
fn draw_url_path(url: &str, host: &str) -> Result<String, u32> {
    if url.starts_with('/') {
        return Ok(url.to_string());
    }
    if !url.contains("://") {
        return Ok(format!("/{url}"));
    }
    let Some((scheme, tail)) = url.split_once("://") else {
        return Err(9423);
    };
    if scheme != "http" && scheme != "https" {
        return Err(9423);
    }
    let Some((url_host, url_path)) = tail.split_once('/') else {
        return Err(9423);
    };
    let url_host = url_host.split(':').next().unwrap_or_default();
    if !url_host.eq_ignore_ascii_case(host) {
        return Err(9401);
    }
    Ok(format!("/{url_path}"))
}

/// One `(k=v,...)` draw entry → its fetch path + the two wire ops it
/// emits. Entries share the option vocabulary: `url` (required) plus
/// positioning (`top/left/bottom/right`), blending (`opacity`,
/// `repeat`, `composite`) and overlay optimization params. Text
/// overlays need a font stack the vendored engine lacks — fail closed.
fn draw_entry(inner: &str, host: &str) -> Result<DrawEntry, u32> {
    let mut url: Option<String> = None;
    let mut draw_op = serde_json::Map::new();
    let mut transform = serde_json::Map::new();
    for pair in split_draw(inner) {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').ok_or(9423u32)?;
        let (k, v) = (k.trim(), v.trim());
        match k {
            "url" => url = Some(unquote(v).to_string()),
            // Text overlays need harfbuzz/freetype — not vendored.
            "text" | "font" | "color" | "size" => return Err(9401),
            "top" | "left" | "bottom" | "right" => {
                let n = parse_num(unquote(v), -16384.0, 16384.0)?;
                draw_op.insert(k.into(), n.into());
            }
            "opacity" => {
                draw_op.insert("opacity".into(), parse_num(unquote(v), 0.0, 1.0)?.into());
            }
            "repeat" => match unquote(v) {
                "true" => {
                    draw_op.insert("repeat".into(), true.into());
                }
                s @ ("x" | "y") => {
                    draw_op.insert("repeat".into(), s.into());
                }
                _ => return Err(9423),
            },
            "composite" => match unquote(v) {
                s @ ("over" | "in" | "atop" | "out" | "xor" | "lighter") => {
                    draw_op.insert("composite".into(), s.into());
                }
                _ => return Err(9423),
            },
            // Overlay dims: integer px or a `"Np"` percent of the base
            // canvas — the op's `percentBasis` marks it for the facade.
            "width" | "height" => {
                let u = unquote(v);
                if let Some(p) = u.strip_suffix('p') {
                    let n = parse_num(p, 0.0, f64::MAX)?;
                    if n <= 0.0 {
                        return Err(9423);
                    }
                    transform.insert(k.into(), u.into());
                } else {
                    transform.insert(k.into(), parse_num(u, 1.0, 16384.0)?.into());
                }
            }
            // `XxY` fractions and `(x=,y=)` groups map onto the
            // binding's coordinate-gravity object like the top-level
            // arm does (facade requires `mode:"remainder"`).
            "gravity" | "g" => {
                if let Some(gi) = group_inner(v) {
                    let (mut x, mut y) = (None, None);
                    for (sk, sv) in group_pairs(gi)? {
                        match sk {
                            "x" => x = Some(parse_num(unquote(sv), 0.0, 1.0)?),
                            "y" => y = Some(parse_num(unquote(sv), 0.0, 1.0)?),
                            _ => return Err(9423),
                        }
                    }
                    let (Some(x), Some(y)) = (x, y) else {
                        return Err(9423);
                    };
                    transform.insert(
                        "gravity".into(),
                        serde_json::json!({"x": x, "y": y, "mode": "remainder"}),
                    );
                } else {
                    let u = unquote(v);
                    match u {
                        "auto" | "entropy" | "face" | "center" | "top" | "bottom" | "left"
                        | "right" => {
                            transform.insert("gravity".into(), u.into());
                        }
                        _ => {
                            let (x, y) = u.split_once('x').ok_or(9423u32)?;
                            let (x, y) = (parse_num(x, 0.0, 1.0)?, parse_num(y, 0.0, 1.0)?);
                            transform.insert(
                                "gravity".into(),
                                serde_json::json!({"x": x, "y": y, "mode": "remainder"}),
                            );
                        }
                    }
                }
            }
            // Inset scalars the top-level `trim`/`border` arms accept —
            // `T;R;B;L` / `TxRxBxL` and `<width>x<hex>` — normalized to
            // the object forms the wire carries.
            "trim" => {
                if let Some(gi) = group_inner(v) {
                    transform.insert("trim".into(), group_to_json(gi)?);
                } else {
                    let u = unquote(v);
                    match u {
                        "border" | "color" => {
                            transform.insert("trim".into(), u.into());
                        }
                        _ => {
                            let sep = if u.contains(';') {
                                ';'
                            } else if u.contains('x') {
                                'x'
                            } else {
                                ','
                            };
                            let nums: Vec<&str> = u.split(sep).collect();
                            if nums.len() != 4 {
                                return Err(9423);
                            }
                            let mut m = serde_json::Map::new();
                            m.insert("mode".into(), "area".into());
                            for (k, s) in ["top", "right", "bottom", "left"].iter().zip(nums) {
                                m.insert((*k).into(), trim_len(s.trim())?);
                            }
                            transform.insert("trim".into(), m.into());
                        }
                    }
                }
            }
            "border" => {
                if let Some(gi) = group_inner(v) {
                    transform.insert("border".into(), group_to_json(gi)?);
                } else {
                    let u = unquote(v);
                    let (w, c) = u.split_once('x').ok_or(9423u32)?;
                    let w = parse_num(w, 0.0, 4096.0)?;
                    if c.len() > 32 {
                        return Err(9423);
                    }
                    transform.insert(
                        "border".into(),
                        serde_json::json!({"width": w, "color": format!("#{c}")}),
                    );
                }
            }
            // Remaining optimization params pass through generically:
            // leaves type by shape, `(k=v,...)` groups become objects —
            // the facade applies its strict per-op validation.
            "fit" | "rotate" | "blur" | "background" | "brightness" | "contrast" | "gamma"
            | "saturation" | "flip" | "sharpen" | "zoom" | "segment" => {
                let val = match group_inner(v) {
                    Some(gi) => group_to_json(gi)?,
                    None => leaf_json(v),
                };
                transform.insert(k.into(), val);
            }
            _ => return Err(9423),
        }
    }
    let Some(u) = url else {
        return Err(9423);
    };
    // Upstream errors when an axis carries both edge offsets.
    if draw_op.contains_key("left") && draw_op.contains_key("right") {
        return Err(9423);
    }
    if draw_op.contains_key("top") && draw_op.contains_key("bottom") {
        return Err(9423);
    }
    let path = draw_url_path(&u, host)?;
    Ok(DrawEntry {
        path,
        draw_op,
        transform,
    })
}

/// Translate `k=v,k=v` URL options into the facade `transforms` op JSON.
/// Structured groups `(k=v,...)` mirror the cf.image object form;
/// the older compact forms keep working. Keys with no vendored-engine
/// support pass through so the facade fails closed with the typed
/// code (`segment`, `zoom`, `anim`→9401).
fn parse_options(raw: &str, headers: &HeaderMap, host: &str) -> Result<Parsed, u32> {
    let mut op = serde_json::Map::new();
    let mut p = Parsed {
        transforms: String::new(),
        output_format: None,
        output_quality: None,
        anim: false,
        info: false,
        wire_metadata: None,
        onerror_redirect: false,
        draws: Vec::new(),
    };
    let mut dpr = 1.0f64;
    let mut width: Option<f64> = None;
    let mut height: Option<f64> = None;
    let mut trim_val: Option<serde_json::Value> = None;
    let mut border_map = serde_json::Map::new();
    let mut padding_seen = false;
    let mut compression_fast = false;
    for pair in split_opts(raw) {
        let (key, value) = pair.split_once('=').ok_or(9423u32)?;
        let decoded = pct_decode(value);
        let v = decoded.trim();
        match key {
            "width" | "w" => {
                if v == "auto" {
                    width = Some(auto_width(
                        headers,
                        &[320.0, 768.0, 960.0, 1200.0],
                        768.0,
                        1200.0,
                    ));
                } else if let Some(inner) = group_inner(v) {
                    let mut mobile = 768.0;
                    let mut desktop = 1200.0;
                    let mut bps = vec![320.0, 768.0, 960.0, 1200.0];
                    for (sk, sv) in group_pairs(inner)? {
                        match sk {
                            "mode" | "m" => {
                                if sv != "auto" {
                                    return Err(9423);
                                }
                            }
                            "mobile" | "mb" => mobile = parse_num(sv, 1.0, 16384.0)?,
                            "desktop" | "dt" => desktop = parse_num(sv, 1.0, 16384.0)?,
                            "breakpoints" | "bp" | "b" => {
                                let Some(list) = group_inner(sv) else {
                                    return Err(9423);
                                };
                                let parsed: Result<Vec<f64>, u32> = split_opts(list)
                                    .into_iter()
                                    .filter(|s| !s.trim().is_empty())
                                    .map(|s| parse_num(s.trim(), 1.0, 16384.0))
                                    .collect();
                                let parsed = parsed?;
                                if parsed.is_empty() {
                                    return Err(9423);
                                }
                                bps = parsed;
                            }
                            _ => return Err(9423),
                        }
                    }
                    width = Some(auto_width(headers, &bps, mobile, desktop));
                } else {
                    width = Some(parse_num(v, 1.0, 16384.0)?);
                }
            }
            "height" | "h" => height = Some(parse_num(v, 1.0, 16384.0)?),
            "dpr" => dpr = parse_num(v, 0.1, 2.0)?,
            "fit" => {
                if let Some(inner) = group_inner(v) {
                    let mut m = serde_json::Map::new();
                    for (sk, sv) in group_pairs(inner)? {
                        match sk {
                            "mode" => {
                                m.insert("mode".into(), sv.into());
                            }
                            "upscale" => {
                                m.insert("upscale".into(), sv.into());
                            }
                            _ => return Err(9423),
                        }
                    }
                    if m.get("mode").is_none() {
                        return Err(9423);
                    }
                    op.insert("fit".into(), m.into());
                } else {
                    match v {
                        "scale-down" | "contain" | "cover" | "crop" | "aspect-crop"
                        | "scale-up" | "pad" | "squeeze" => {
                            op.insert("fit".into(), v.into());
                        }
                        _ => return Err(9423),
                    }
                }
            }
            "flip" => match v {
                "h" | "v" | "hv" => {
                    op.insert("flip".into(), v.into());
                }
                _ => return Err(9423),
            },
            "gravity" | "g" => {
                // Named anchors pass through (`face` reaches the facade,
                // which fails closed — no detection is vendored); `XxY`
                // fractions and the `(x=,y=)` group map onto the
                // binding's coordinate-gravity object.
                if let Some(inner) = group_inner(v) {
                    let (mut x, mut y) = (None, None);
                    for (sk, sv) in group_pairs(inner)? {
                        match sk {
                            "x" => x = Some(parse_num(sv, 0.0, 1.0)?),
                            "y" => y = Some(parse_num(sv, 0.0, 1.0)?),
                            _ => return Err(9423),
                        }
                    }
                    let (Some(x), Some(y)) = (x, y) else {
                        return Err(9423);
                    };
                    op.insert(
                        "gravity".into(),
                        serde_json::json!({"x": x, "y": y, "mode": "remainder"}),
                    );
                } else {
                    match v {
                        "auto" | "entropy" | "face" | "center" | "top" | "bottom" | "left"
                        | "right" => {
                            op.insert("gravity".into(), v.into());
                        }
                        _ => {
                            let (x, y) = v.split_once('x').ok_or(9423u32)?;
                            let (x, y) = (parse_num(x, 0.0, 1.0)?, parse_num(y, 0.0, 1.0)?);
                            op.insert(
                                "gravity".into(),
                                serde_json::json!({"x": x, "y": y, "mode": "remainder"}),
                            );
                        }
                    }
                }
            }
            "rotate" => {
                let d = parse_num(v, 0.0, 360.0)?;
                op.insert("rotate".into(), (d as u32).into());
            }
            "blur" => {
                op.insert("blur".into(), parse_num(v, 1.0, 250.0)?.into());
            }
            "sharpen" => {
                if let Some(inner) = group_inner(v) {
                    let mut m = serde_json::Map::new();
                    let mut mode = "box";
                    for (sk, sv) in group_pairs(inner)? {
                        match sk {
                            "mode" => match sv {
                                "box" | "gaussian" => mode = sv,
                                _ => return Err(9423),
                            },
                            "amount" => {
                                m.insert("amount".into(), parse_num(sv, 0.0, 10.0)?.into());
                            }
                            "sigma" => {
                                let s = parse_num(sv, f64::EPSILON, 5.0)?;
                                m.insert("sigma".into(), s.into());
                            }
                            "threshold" => {
                                let t = parse_num(sv, 0.0, 255.0)?;
                                if t.fract() != 0.0 {
                                    return Err(9423);
                                }
                                m.insert("threshold".into(), (t as u32).into());
                            }
                            _ => return Err(9423),
                        }
                    }
                    if m.get("amount").is_none() {
                        return Err(9423);
                    }
                    if mode == "gaussian" {
                        if m.get("sigma").is_none() {
                            return Err(9423);
                        }
                    } else if m.get("sigma").is_some() || m.get("threshold").is_some() {
                        return Err(9423);
                    }
                    m.insert("mode".into(), mode.into());
                    op.insert("sharpen".into(), m.into());
                } else {
                    op.insert("sharpen".into(), parse_num(v, 0.0, 10.0)?.into());
                }
            }
            "brightness" => {
                op.insert("brightness".into(), parse_num(v, 0.0, 3.0)?.into());
            }
            "contrast" => {
                op.insert("contrast".into(), parse_num(v, 0.0, 3.0)?.into());
            }
            "gamma" => {
                op.insert("gamma".into(), parse_num(v, 0.01, 10.0)?.into());
            }
            "saturation" => {
                op.insert("saturation".into(), parse_num(v, 0.0, 3.0)?.into());
            }
            "background" => {
                if v.len() > 64 {
                    return Err(9423);
                }
                op.insert("background".into(), v.into());
            }
            "trim"
            | "trim.mode"
            | "trim.top"
            | "trim.right"
            | "trim.bottom"
            | "trim.left"
            | "trim.width"
            | "trim.height"
            | "trim.border.color"
            | "trim.border.tolerance"
            | "trim.border.keep" => {
                // All trim spellings accumulate into one object emitted
                // after the loop so dpr can scale the pixel counts:
                // `trim=color|border`, `trim=T;R;B;L` / `TxRxBxL` insets,
                // `trim=(mode=..., ...)` groups, and the legacy dotted
                // `trim.X=` / `trim.border.X=` keys.
                let map = trim_val
                    .get_or_insert_with(|| serde_json::json!({}))
                    .as_object_mut()
                    .ok_or(9423u32)?;
                let set_mode = |m: &mut serde_json::Map<String, serde_json::Value>,
                                mode: &str|
                 -> Result<(), u32> {
                    if let Some(prev) = m.get("mode").and_then(|x| x.as_str()) {
                        if prev != mode {
                            return Err(9423);
                        }
                    }
                    m.insert("mode".into(), mode.into());
                    Ok(())
                };
                if key == "trim" {
                    if v == "border" || v == "color" {
                        set_mode(map, "color")?;
                    } else if let Some(inner) = group_inner(v) {
                        let (mut color_keys, mut area_keys) = (false, false);
                        for (sk, sv) in group_pairs(inner)? {
                            match sk {
                                "mode" => match sv {
                                    "color" | "area" => set_mode(map, sv)?,
                                    _ => return Err(9423),
                                },
                                "color" => {
                                    if sv.len() > 64 {
                                        return Err(9423);
                                    }
                                    color_keys = true;
                                    map.insert("color".into(), sv.into());
                                }
                                "tolerance" => {
                                    let t = parse_num(sv, 0.0, 255.0)?;
                                    if t.fract() != 0.0 {
                                        return Err(9423);
                                    }
                                    color_keys = true;
                                    map.insert("tolerance".into(), (t as u32).into());
                                }
                                "keep" => {
                                    color_keys = true;
                                    map.insert("keep".into(), parse_num(sv, 0.0, 16384.0)?.into());
                                }
                                "top" | "right" | "bottom" | "left" | "width" | "height" => {
                                    area_keys = true;
                                    map.insert(sk.into(), trim_len(sv)?);
                                }
                                _ => return Err(9423),
                            }
                        }
                        // color and area settings never mix in one mode.
                        match map.get("mode").and_then(|m| m.as_str()) {
                            Some("color") if area_keys => return Err(9423),
                            Some("area") if color_keys => return Err(9423),
                            None if color_keys && area_keys => return Err(9423),
                            _ => {}
                        }
                    } else {
                        let sep = if v.contains(';') {
                            ';'
                        } else if v.contains('x') {
                            'x'
                        } else {
                            ','
                        };
                        let nums: Vec<&str> = v.split(sep).collect();
                        if nums.len() != 4 {
                            return Err(9423);
                        }
                        set_mode(map, "area")?;
                        for (k, s) in ["top", "right", "bottom", "left"].iter().zip(nums) {
                            map.insert((*k).into(), trim_len(s.trim())?);
                        }
                    }
                } else if let Some(sub) = key.strip_prefix("trim.border.") {
                    set_mode(map, "color")?;
                    match sub {
                        "color" => {
                            if v.len() > 64 {
                                return Err(9423);
                            }
                            map.insert("color".into(), v.into());
                        }
                        "tolerance" => {
                            let t = parse_num(v, 0.0, 255.0)?;
                            if t.fract() != 0.0 {
                                return Err(9423);
                            }
                            map.insert("tolerance".into(), (t as u32).into());
                        }
                        "keep" => {
                            map.insert("keep".into(), parse_num(v, 0.0, 16384.0)?.into());
                        }
                        _ => return Err(9423),
                    }
                } else if key == "trim.mode" {
                    match v {
                        "color" | "area" => set_mode(map, v)?,
                        _ => return Err(9423),
                    }
                } else {
                    // trim.top/right/bottom/left/width/height
                    set_mode(map, "area")?;
                    let sub = key.strip_prefix("trim.").ok_or(9423u32)?;
                    map.insert(sub.into(), trim_len(v)?);
                }
            }
            "border" => {
                // `<width>x<color>` (uniform) or the
                // `(color=,width=|top,right,bottom,left)` group — width
                // keys accept px ints and `"Np"` percents.
                if let Some(inner) = group_inner(v) {
                    for (sk, sv) in group_pairs(inner)? {
                        match sk {
                            "color" => {
                                if sv.len() > 64 {
                                    return Err(9423);
                                }
                                border_map.insert("color".into(), sv.into());
                            }
                            "width" | "top" | "right" | "bottom" | "left" => {
                                border_map.insert(sk.into(), trim_len(sv)?);
                            }
                            _ => return Err(9423),
                        }
                    }
                } else {
                    let (w, c) = v.split_once('x').ok_or(9423u32)?;
                    let w = parse_num(w, 0.0, 4096.0)?;
                    if c.len() > 32 {
                        return Err(9423);
                    }
                    border_map.insert("width".into(), w.into());
                    border_map.insert("color".into(), format!("#{c}").into());
                }
            }
            "padding" => {
                // Uniform or `topxrightxbottomxleft`; painted with the
                // `background` color (or transparent) at emit time.
                let nums: Vec<&str> = v.split('x').collect();
                let (t, r, b, l) = match nums.len() {
                    1 => {
                        let n = parse_num(nums[0], 0.0, 4096.0)?;
                        (n, n, n, n)
                    }
                    4 => (
                        parse_num(nums[0], 0.0, 4096.0)?,
                        parse_num(nums[1], 0.0, 4096.0)?,
                        parse_num(nums[2], 0.0, 4096.0)?,
                        parse_num(nums[3], 0.0, 4096.0)?,
                    ),
                    _ => return Err(9423),
                };
                border_map.insert("top".into(), t.into());
                border_map.insert("right".into(), r.into());
                border_map.insert("bottom".into(), b.into());
                border_map.insert("left".into(), l.into());
                padding_seen = true;
            }
            "compression" => {
                if v != "fast" {
                    return Err(9423);
                }
                compression_fast = true;
            }
            "format" | "f" => match v {
                "auto" => {
                    // Content negotiation on the inbound Accept header —
                    // CF prefers avif, then webp, else the source format
                    // (jpeg is our neutral fallback).
                    let accept = headers
                        .get(header::ACCEPT)
                        .and_then(|a| a.to_str().ok())
                        .unwrap_or_default();
                    p.output_format = Some(
                        if accept.contains("image/avif") {
                            "image/avif"
                        } else if accept.contains("image/webp") {
                            "image/webp"
                        } else {
                            "image/jpeg"
                        }
                        .to_string(),
                    );
                }
                "jpeg" | "jpg" => p.output_format = Some("image/jpeg".into()),
                "png" => p.output_format = Some("image/png".into()),
                "webp" => p.output_format = Some("image/webp".into()),
                "avif" => p.output_format = Some("image/avif".into()),
                // gif/rgb/rgba pass through so the facade returns the
                // typed 9401 rather than a hard parser error.
                "gif" | "rgb" | "rgba" => {
                    p.output_format = Some(format!("image/{v}"));
                }
                _ => return Err(9423),
            },
            "quality" | "q" => {
                if let Some(inner) = group_inner(v) {
                    let mut slow: Option<u32> = None;
                    for (sk, sv) in group_pairs(inner)? {
                        match sk {
                            "level" | "v" => p.output_quality = Some(quality_level(sv)?),
                            "slow-connection" | "sc" => slow = Some(quality_level(sv)?),
                            _ => return Err(9423),
                        }
                    }
                    if let Some(s) = slow.filter(|_| slow_connection(headers)) {
                        p.output_quality = Some(s);
                    }
                } else {
                    p.output_quality = Some(quality_level(v)?);
                }
            }
            "slow-connection-quality" | "scq" => {
                if slow_connection(headers) {
                    p.output_quality = Some(quality_level(v)?);
                }
            }
            "anim" => {
                p.anim = match v {
                    "true" => true,
                    "false" => false,
                    _ => return Err(9423),
                };
            }
            "metadata" => match v {
                // The preservation enum rides the transform wire;
                // `exif` keeps our `/info` introspection extension.
                "none" | "copyright" | "keep" => p.wire_metadata = Some(v.into()),
                "exif" => p.info = true,
                _ => return Err(9423),
            },
            "onerror" => {
                p.onerror_redirect = v == "redirect";
                if !p.onerror_redirect {
                    return Err(9423);
                }
            }
            // Binding-op keys with no URL meaning here; let the facade's
            // own fail-closed path type the error.
            "zoom" => {
                op.insert("zoom".into(), parse_num(v, 0.1, 8.0)?.into());
            }
            "segment" => {
                op.insert("segment".into(), v.into());
            }
            "draw" => {
                // `draw=((k=v,...),(k=v,...))` — parenthesized overlay
                // entries sharing the option grammar; a bare
                // `draw=(k=v,...)` reads as a single entry.
                let Some(inner) = group_inner(v) else {
                    return Err(9423);
                };
                let elems: Vec<&str> = split_draw(inner)
                    .into_iter()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .collect();
                let all_groups =
                    !elems.is_empty() && elems.iter().all(|e| group_inner(e).is_some());
                let entry_inners: Vec<&str> = if all_groups {
                    elems.iter().filter_map(|e| group_inner(e)).collect()
                } else {
                    vec![inner]
                };
                if entry_inners.len() > 16 {
                    return Err(9423);
                }
                for ei in entry_inners {
                    p.draws.push(draw_entry(ei, host)?);
                }
            }
            _ => return Err(9423),
        }
    }
    if let Some(w) = width {
        op.insert("width".into(), ((w * dpr).round().max(1.0) as u64).into());
    }
    if let Some(h) = height {
        op.insert("height".into(), ((h * dpr).round().max(1.0) as u64).into());
    }
    if let Some(mut tv) = trim_val {
        scale_px(&mut tv, dpr);
        op.insert("trim".into(), tv);
    }
    if !border_map.is_empty() {
        let mut bv = serde_json::Value::Object(border_map);
        scale_px(&mut bv, dpr);
        if padding_seen && bv.get("color").is_none() {
            bv["color"] = op
                .get("background")
                .cloned()
                .unwrap_or_else(|| "rgba(0,0,0,0)".into());
        }
        op.insert("border".into(), bv);
    }
    if compression_fast {
        // `compression=fast` picks the quickest format upstream —
        // JPEG — overriding an explicit `format`.
        p.output_format = Some("image/jpeg".into());
    }
    // `[mainOp, overOp1?, drawOp1, overOp2?, drawOp2, ...]` — the ops
    // apply in order so the last draw entry composites topmost. Pixel
    // offsets and px dims scale with dpr like the main op's values.
    let scaled = |m: &serde_json::Map<String, serde_json::Value>| {
        let mut v = serde_json::Value::Object(m.clone());
        scale_px(&mut v, dpr);
        match v {
            serde_json::Value::Object(o) => o,
            _ => serde_json::Map::new(),
        }
    };
    let mut ops = vec![serde_json::Value::Object(op)];
    for (i, e) in p.draws.iter().enumerate() {
        let mut ov = scaled(&e.transform);
        if !ov.is_empty() {
            ov.insert("imageIndex".into(), (i + 1).into());
            ov.insert("percentBasis".into(), 0.into());
            ops.push(serde_json::Value::Object(ov));
        }
        let mut d = scaled(&e.draw_op);
        d.insert("drawImageIndex".into(), (i + 1).into());
        d.insert("targetImageIndex".into(), 0.into());
        ops.push(serde_json::Value::Object(d));
    }
    p.transforms = serde_json::json!(ops).to_string();
    Ok(p)
}

async fn http1(
    io: UpstreamIo,
    req: Request<http_body_util::combinators::UnsyncBoxBody<Bytes, axum::Error>>,
) -> Result<Response<axum::body::Body>, ()> {
    let (mut sender, conn) = hyper::client::conn::http1::handshake::<
        _,
        http_body_util::combinators::UnsyncBoxBody<Bytes, axum::Error>,
    >(hyper_util::rt::TokioIo::new(io))
    .await
    .map_err(|_| ())?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    sender.ready().await.map_err(|_| ())?;
    let resp = sender.send_request(req).await.map_err(|_| ())?;
    let (parts, body) = resp.into_parts();
    Ok(Response::from_parts(parts, Body::new(body)))
}

/// Sibling of the tenant-ingress socket: `<workerId>.sock` →
/// `<workerId>-images.sock` in the same generation socket dir.
fn images_endpoint(worker_ep: &WorkerEndpoint) -> Option<WorkerEndpoint> {
    // Refutable only under `dev-stub` (the Tcp variant) — production
    // builds see a single-variant enum.
    #[allow(irrefutable_let_patterns)]
    let WorkerEndpoint::Unix(path) = worker_ep
    else {
        return None;
    };
    let name = path.file_name()?.to_str()?.strip_suffix(".sock")?;
    Some(WorkerEndpoint::Unix(
        path.with_file_name(format!("{name}-images.sock")),
    ))
}

async fn run_transform(
    headers: &HeaderMap,
    src_path: &str,
    parsed: Parsed,
    target: &RouteTarget,
    runtime: &Arc<TenantRuntime>,
    host: &str,
) -> Result<Response<Body>, IngressError> {
    let slot = runtime
        .endpoint_slot(&target.worker_id)
        .ok_or(IngressError::TenantUnavailable("tenant not ready"))?;
    let worker_ep = slot.endpoint().clone();
    let accept = headers
        .get(header::ACCEPT)
        .cloned()
        .unwrap_or_else(|| header::HeaderValue::from_static("*/*"));

    // Errors surface as u16 codes: 4xx/5xx upstream statuses pass
    // through, transport/build failures collapse to 9411.
    let upstream: Result<Response<Body>, u16> = tokio::time::timeout(UPSTREAM_TIMEOUT, async {
        // 1. Fetch the source through the tenant's own worker socket —
        //    this is the same-origin semantic (worker, assets, or the
        //    platform asset route all resolve naturally).
        let get = Request::builder()
            .method("GET")
            .uri(src_path)
            .header(header::HOST, host)
            .header(header::ACCEPT, "*/*")
            .body(
                Full::new(Bytes::new())
                    .map_err(axum::Error::new)
                    .boxed_unsync(),
            )
            .map_err(|_| 9411u16)?;
        let io = worker_ep.connect().await.map_err(|_| 9411u16)?;
        let src_resp = http1(io, get).await.map_err(|_| 9411u16)?;
        if !src_resp.status().is_success() {
            return Err(src_resp.status().as_u16());
        }
        let src = src_resp
            .into_body()
            .collect()
            .await
            .map_err(|_| 9411u16)?
            .to_bytes();
        if src.len() > MAX_SRC_BYTES {
            return Err(9424u16);
        }

        // Upstream never rasterizes SVG: the image-resizing service
        // sanitizes the source (svg-hush) and ignores every transform
        // option. Malformed markup gets the corrupt-image error.
        if looks_like_svg(&src) {
            return sanitize_svg(&src)
                .map(|out| {
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "image/svg+xml")
                        .body(Body::from(out))
                        .unwrap_or_else(|_| Response::new(Body::empty()))
                })
                .or_else(|_| Ok(error_body(9420, StatusCode::UNSUPPORTED_MEDIA_TYPE)));
        }

        // Draw overlays fetch through the same tenant socket — their
        // same-origin paths resolve exactly like the source did, and
        // the same status/size rules apply.
        let mut overlays: Vec<Bytes> = Vec::with_capacity(parsed.draws.len());
        for e in &parsed.draws {
            let get = Request::builder()
                .method("GET")
                .uri(&e.path)
                .header(header::HOST, host)
                .header(header::ACCEPT, "*/*")
                .body(
                    Full::new(Bytes::new())
                        .map_err(axum::Error::new)
                        .boxed_unsync(),
                )
                .map_err(|_| 9411u16)?;
            let io = worker_ep.connect().await.map_err(|_| 9411u16)?;
            let over_resp = http1(io, get).await.map_err(|_| 9411u16)?;
            if !over_resp.status().is_success() {
                return Err(over_resp.status().as_u16());
            }
            let over = over_resp
                .into_body()
                .collect()
                .await
                .map_err(|_| 9411u16)?
                .to_bytes();
            if over.len() > MAX_SRC_BYTES {
                return Err(9424u16);
            }
            overlays.push(over);
        }

        // 3. Multipart POST to the facade-images socket.
        let boundary = "vrtcdnimage0a1b2c3d4e5f60718293a4b5c6d7e8f9";
        let mut body = Vec::with_capacity(src.len() + 1024);
        let mut field = |name: &str, filename: Option<&str>, value: &[u8]| {
            body.extend_from_slice(
                format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"")
                    .as_bytes(),
            );
            if let Some(f) = filename {
                body.extend_from_slice(
                    format!("; filename=\"{f}\"\r\nContent-Type: application/octet-stream")
                        .as_bytes(),
                );
            }
            body.extend_from_slice(b"\r\n\r\n");
            body.extend_from_slice(value);
            body.extend_from_slice(b"\r\n");
        };
        field("image", Some("source"), &src);
        field("transforms", None, parsed.transforms.as_bytes());
        if let Some(f) = &parsed.output_format {
            field("output_format", None, f.as_bytes());
        }
        if let Some(q) = parsed.output_quality {
            field("output_quality", None, q.to_string().as_bytes());
        }
        if parsed.anim {
            field("anim", None, b"true");
        }
        if let Some(md) = &parsed.wire_metadata {
            field("metadata", None, md.as_bytes());
        }
        for over in &overlays {
            field("draw_image", Some("overlay"), over);
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        let ep = images_endpoint(&worker_ep).ok_or(9411u16)?;
        let post = Request::builder()
            .method("POST")
            .uri(if parsed.info { "/info" } else { "/transform" })
            .header(header::HOST, "vrt-images.internal")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .header(header::ACCEPT, accept)
            .body(
                Full::new(Bytes::from(body))
                    .map_err(axum::Error::new)
                    .boxed_unsync(),
            )
            .map_err(|_| 9411u16)?;
        let io = ep.connect().await.map_err(|_| 9411u16)?;
        http1(io, post).await.map_err(|_| 9411u16)
    })
    .await
    .unwrap_or(Err(9411));

    let redirect = || {
        Response::builder()
            .status(StatusCode::FOUND)
            .header(header::LOCATION, src_path)
            .body(Body::empty())
            .unwrap_or_else(|_| Response::new(Body::empty()))
    };
    match upstream {
        Ok(resp) => {
            // Typed facade errors keep their status; the body becomes
            // CF's `ERROR <code>` text shape.
            if let Some(err) = resp.headers().get("cf-images-binding") {
                let code = err
                    .to_str()
                    .ok()
                    .and_then(|v| v.strip_prefix("err="))
                    .and_then(|v| v.parse::<u32>().ok())
                    .unwrap_or(9523);
                if parsed.onerror_redirect {
                    return Ok(redirect());
                }
                return Ok(error_body(code, resp.status()));
            }
            Ok(resp)
        }
        Err(status) => {
            if parsed.onerror_redirect {
                return Ok(redirect());
            }
            Ok(error_body(
                if status == 9424 { 9424 } else { 9411 },
                StatusCode::from_u16(status.clamp(400, 599)).unwrap_or(StatusCode::BAD_GATEWAY),
            ))
        }
    }
}

/// SVG has no binary magic — sniff the leading markup the same way
/// `facade/images-wire.ts` does: `<svg` or `<?xml` after whitespace.
/// UTF-16 BOMs decode like the sanitizer (xml-rs) does; UTF-8 damage
/// stays a no-match, same as the lossy TextDecoder.
fn looks_like_svg(src: &[u8]) -> bool {
    let head = if src.starts_with(&[0xFF, 0xFE]) {
        utf16_head(&src[..src.len().min(8192)], u16::from_le_bytes)
    } else if src.starts_with(&[0xFE, 0xFF]) {
        utf16_head(&src[..src.len().min(8192)], u16::from_be_bytes)
    } else {
        String::from_utf8_lossy(&src[..src.len().min(4096)]).into_owned()
    };
    let t = head.trim_start_matches('\u{FEFF}').trim_start();
    t.starts_with("<svg") || t.starts_with("<?xml")
}

fn utf16_head(src: &[u8], f: fn([u8; 2]) -> u16) -> String {
    let units: Vec<u16> = src.chunks_exact(2).map(|c| f([c[0], c[1]])).collect();
    String::from_utf16_lossy(&units)
}

/// svg-hush pass: the vendored crate is the upstream sanitizer
/// verbatim — allowlisted elements/attributes, URLs relativized to
/// same-origin, `data:` limited to image/{jpeg,png,gif}, scripts and
/// foreign namespaces gone. Err on anything not well-formed SVG XML.
fn sanitize_svg(src: &[u8]) -> Result<Vec<u8>, svg_hush::FError> {
    let mut filter = svg_hush::Filter::new();
    filter.set_data_url_filter(svg_hush::data_url_filter::allow_standard_images);
    let mut out = Vec::new();
    filter.filter(src, &mut out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(raw: &str) -> Parsed {
        parse_options(raw, &HeaderMap::new(), "example.com").expect(raw)
    }

    #[test]
    fn option_table_maps_to_transform_json() {
        let p = opts("width=200,height=100,fit=cover,gravity=auto,quality=80,format=webp");
        let ops: serde_json::Value = serde_json::from_str(&p.transforms).unwrap();
        let op = &ops[0];
        assert_eq!(op["width"], 200);
        assert_eq!(op["height"], 100);
        assert_eq!(op["fit"], "cover");
        assert_eq!(op["gravity"], "auto");
        assert_eq!(p.output_format.as_deref(), Some("image/webp"));
        assert_eq!(p.output_quality, Some(80));
    }

    #[test]
    fn dpr_scales_dimensions() {
        let p = opts("width=200,dpr=2");
        let ops: serde_json::Value = serde_json::from_str(&p.transforms).unwrap();
        assert_eq!(ops[0]["width"], 400);
    }

    #[test]
    fn coordinate_gravity_maps_to_xy_object() {
        let p = opts("gravity=0.2x0.8");
        let ops: serde_json::Value = serde_json::from_str(&p.transforms).unwrap();
        assert_eq!(ops[0]["gravity"]["x"], 0.2);
        assert_eq!(ops[0]["gravity"]["mode"], "remainder");
    }

    #[test]
    fn bad_and_unknown_options_fail_closed() {
        for raw in [
            "width=0",
            "width=",
            "bogus=1",
            "fit=stretch",
            "quality=101",
            "rotate=abc",
        ] {
            assert!(
                parse_options(raw, &HeaderMap::new(), "example.com").is_err(),
                "{raw}"
            );
        }
    }

    #[test]
    fn format_auto_negotiates_on_accept() {
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT, "image/avif,image/webp".parse().unwrap());
        let p = parse_options("format=auto", &h, "example.com").unwrap();
        assert_eq!(p.output_format.as_deref(), Some("image/avif"));
        let p = parse_options("format=auto", &HeaderMap::new(), "example.com").unwrap();
        assert_eq!(p.output_format.as_deref(), Some("image/jpeg"));
    }

    fn op_of(p: &Parsed) -> serde_json::Value {
        serde_json::from_str::<serde_json::Value>(&p.transforms).unwrap()[0].clone()
    }

    #[test]
    fn sharpen_group_and_scalar() {
        let g = op_of(&opts(
            "sharpen=(mode=gaussian,amount=1,sigma=1.5,threshold=0)",
        ));
        assert_eq!(
            g["sharpen"],
            serde_json::json!({"mode": "gaussian", "amount": 1.0, "sigma": 1.5, "threshold": 0})
        );
        let b = op_of(&opts("sharpen=(mode=box,amount=5)"));
        assert_eq!(
            b["sharpen"],
            serde_json::json!({"mode": "box", "amount": 5.0})
        );
        assert_eq!(op_of(&opts("sharpen=5"))["sharpen"], 5.0);
        for raw in [
            "sharpen=(mode=gaussian,amount=1)", // sigma required
            "sharpen=(amount=1,sigma=1)",       // sigma without gaussian
            "sharpen=(mode=mild,amount=1)",
            "sharpen=(sigma=1)", // amount required
            "sharpen=(mode=box,amount=1,threshold=3)",
        ] {
            assert!(
                parse_options(raw, &HeaderMap::new(), "example.com").is_err(),
                "{raw}"
            );
        }
    }

    #[test]
    fn trim_all_spellings() {
        // Structured color mode + the renamed string.
        let c = op_of(&opts(
            "trim=(mode=color,color=%23ffffff,tolerance=10,keep=5)",
        ));
        assert_eq!(
            c["trim"],
            serde_json::json!({"mode": "color", "color": "#ffffff", "tolerance": 10, "keep": 5.0})
        );
        for s in ["color", "border"] {
            assert_eq!(op_of(&opts(&format!("trim={s}")))["trim"]["mode"], "color");
        }
        // Side insets: group, %-lengths, fractions, and both legacy
        // separators.
        let a = op_of(&opts(
            "trim=(mode=area,top=10p,right=20p,bottom=10p,left=20p)",
        ));
        assert_eq!(a["trim"]["top"], "10p");
        assert_eq!(a["trim"]["right"], "20p");
        let f = op_of(&opts("trim=(mode=area,top=0.1,left=0.2)"));
        assert_eq!(f["trim"]["top"], 0.1);
        assert_eq!(f["trim"]["left"], 0.2);
        for sep in [";", "x"] {
            let s = op_of(&opts(&format!("trim=10{sep}20{sep}10{sep}20")));
            assert_eq!(s["trim"]["top"], 10.0, "{sep}");
            assert_eq!(s["trim"]["left"], 20.0, "{sep}");
        }
        // Bare side key infers area; rect form keeps its keys.
        assert_eq!(op_of(&opts("trim=(left=800)"))["trim"]["left"], 800.0);
        let r = op_of(&opts(
            "trim=(mode=area,left=10,top=10,width=500,height=300)",
        ));
        assert_eq!(r["trim"]["width"], 500.0);
        // Legacy dotted keys fold into the same object.
        let d = op_of(&opts("trim.top=5,trim.left=10"));
        assert_eq!(d["trim"]["top"], 5.0);
        assert_eq!(d["trim"]["left"], 10.0);
        let db = op_of(&opts(
            "trim=color,trim.border.tolerance=30,trim.border.keep=2",
        ));
        assert_eq!(
            db["trim"],
            serde_json::json!({"mode": "color", "tolerance": 30, "keep": 2.0})
        );
        for raw in [
            "trim=(mode=color,top=5)",
            "trim=(mode=bogus)",
            "trim=1;2;3",
            "trim=(left=abc)",
            "trim=(mode=area,color=%23fff)",
        ] {
            assert!(
                parse_options(raw, &HeaderMap::new(), "example.com").is_err(),
                "{raw}"
            );
        }
    }

    #[test]
    fn fit_new_modes_and_group() {
        for m in ["aspect-crop", "scale-up"] {
            assert_eq!(op_of(&opts(&format!("fit={m}")))["fit"], m);
        }
        let g = op_of(&opts("fit=(mode=contain,upscale=generate)"));
        assert_eq!(
            g["fit"],
            serde_json::json!({"mode": "contain", "upscale": "generate"})
        );
        assert!(parse_options("fit=(upscale=generate)", &HeaderMap::new(), "example.com").is_err());
    }

    #[test]
    fn gravity_group_and_alias() {
        let g = op_of(&opts("gravity=(x=0.5,y=0.3)"));
        assert_eq!(g["gravity"]["x"], 0.5);
        assert_eq!(g["gravity"]["y"], 0.3);
        assert_eq!(op_of(&opts("g=auto"))["gravity"], "auto");
        assert!(parse_options("gravity=(x=0.5)", &HeaderMap::new(), "example.com").is_err());
        assert!(parse_options("gravity=(x=1.5,y=0.3)", &HeaderMap::new(), "example.com").is_err());
    }

    #[test]
    fn border_group_percent_and_padding() {
        let b = op_of(&opts("border=(color=%23ffffff,width=10)"));
        assert_eq!(
            b["border"],
            serde_json::json!({"color": "#ffffff", "width": 10.0})
        );
        let p = op_of(&opts("border=(width=10p)"));
        assert_eq!(p["border"]["width"], "10p");
        // rgba() commas stay inside the group.
        let rgba = op_of(&opts(
            "border=(color=rgba(0,0,0,0),top=5,left=10),width=200",
        ));
        assert_eq!(rgba["border"]["color"], "rgba(0,0,0,0)");
        assert_eq!(rgba["border"]["top"], 5.0);
        assert_eq!(rgba["width"], 200);
        // Legacy WxCOLOR still works.
        let legacy = op_of(&opts("border=5xFF0000"));
        assert_eq!(
            legacy["border"],
            serde_json::json!({"width": 5.0, "color": "#FF0000"})
        );
        // padding defaults its color to transparent.
        let pad = op_of(&opts("padding=10"));
        assert_eq!(
            pad["border"],
            serde_json::json!({"top": 10.0, "right": 10.0, "bottom": 10.0, "left": 10.0, "color": "rgba(0,0,0,0)"})
        );
        let pad_bg = op_of(&opts("padding=10,background=%23ff0000"));
        assert_eq!(pad_bg["border"]["color"], "#ff0000");
    }

    #[test]
    fn quality_tiers_groups_and_slow_connection() {
        assert_eq!(opts("quality=low").output_quality, Some(40));
        assert_eq!(opts("quality=medium-high").output_quality, Some(80));
        assert_eq!(opts("q=50").output_quality, Some(50));
        // slow-connection applies only with slow client hints.
        let mut slow_h = HeaderMap::new();
        slow_h.insert("ect", "2g".parse().unwrap());
        let p = parse_options(
            "quality=(level=80,slow-connection=40)",
            &slow_h,
            "example.com",
        )
        .unwrap();
        assert_eq!(p.output_quality, Some(40));
        let fast = parse_options(
            "quality=(level=80,slow-connection=40)",
            &HeaderMap::new(),
            "example.com",
        )
        .unwrap();
        assert_eq!(fast.output_quality, Some(80));
        let short = parse_options("q=(v=80,sc=40)", &slow_h, "example.com").unwrap();
        assert_eq!(short.output_quality, Some(40));
        // Top-level scq key + the other hint channels.
        let mut rtt = HeaderMap::new();
        rtt.insert("rtt", "400".parse().unwrap());
        assert_eq!(
            parse_options("slow-connection-quality=30", &rtt, "example.com")
                .unwrap()
                .output_quality,
            Some(30)
        );
        let mut sd = HeaderMap::new();
        sd.insert("save-data", "on".parse().unwrap());
        assert_eq!(
            parse_options("scq=low", &sd, "example.com")
                .unwrap()
                .output_quality,
            Some(40)
        );
        let mut dl = HeaderMap::new();
        dl.insert("downlink", "1".parse().unwrap());
        assert_eq!(
            parse_options("scq=30", &dl, "example.com")
                .unwrap()
                .output_quality,
            Some(30)
        );
        // scq ignored on fast connections.
        assert_eq!(
            parse_options("scq=30", &HeaderMap::new(), "example.com")
                .unwrap()
                .output_quality,
            None
        );
    }

    #[test]
    fn width_auto_client_hints_and_ua() {
        // Viewport-Width picks the smallest breakpoint >= it.
        let mut h = HeaderMap::new();
        h.insert("viewport-width", "700".parse().unwrap());
        let p = parse_options("width=auto", &h, "example.com").unwrap();
        assert_eq!(op_of(&p)["width"], 768);
        // Beyond all breakpoints → the largest.
        h.insert("viewport-width", "4000".parse().unwrap());
        let p =
            parse_options("width=(mode=auto,breakpoints=(400,800))", &h, "example.com").unwrap();
        assert_eq!(op_of(&p)["width"], 800);
        // UA fallback: mobile → 768, desktop → 1200 (overridable).
        let mut mob = HeaderMap::new();
        mob.insert(
            header::USER_AGENT,
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0)".parse().unwrap(),
        );
        assert_eq!(
            op_of(&parse_options("width=auto", &mob, "example.com").unwrap())["width"],
            768
        );
        assert_eq!(
            op_of(&parse_options("width=(m=auto,mb=600,dt=1400)", &mob, "example.com").unwrap())
                ["width"],
            600
        );
        assert_eq!(op_of(&opts("width=auto"))["width"], 1200);
        assert!(parse_options("width=(mode=fixed)", &HeaderMap::new(), "example.com").is_err());
    }

    #[test]
    fn metadata_wire_and_compression_and_flip() {
        for m in ["none", "copyright", "keep"] {
            let p = opts(&format!("metadata={m}"));
            assert_eq!(p.wire_metadata.as_deref(), Some(m));
            assert!(!p.info);
        }
        let p = opts("metadata=exif");
        assert!(p.info);
        assert!(p.wire_metadata.is_none());
        for raw in ["metadata=true", "metadata=false"] {
            assert!(
                parse_options(raw, &HeaderMap::new(), "example.com").is_err(),
                "{raw}"
            );
        }
        assert_eq!(
            opts("compression=fast").output_format.as_deref(),
            Some("image/jpeg")
        );
        assert_eq!(
            opts("format=webp,compression=fast")
                .output_format
                .as_deref(),
            Some("image/jpeg")
        );
        for f in ["h", "v", "hv"] {
            assert_eq!(op_of(&opts(&format!("flip={f}")))["flip"], f);
        }
        assert!(parse_options("flip=d", &HeaderMap::new(), "example.com").is_err());
    }

    #[test]
    fn dpr_scales_pixel_lengths() {
        let p = opts("dpr=2,trim=10;10;10;10,border=(width=5),padding=6");
        let op = op_of(&p);
        assert_eq!(op["trim"]["top"], 20.0);
        assert_eq!(op["border"]["width"], 10.0);
        // padding sides merge with the border width; the padding color
        // fills in transparent when none is set.
        assert_eq!(op["border"]["top"], 12.0);
        assert_eq!(op["border"]["color"], "rgba(0,0,0,0)");
        // Percents and fractions do not scale — the facade resolves
        // them against the axis.
        let p = opts("dpr=2,trim=(mode=area,top=10p,left=0.1)");
        let op = op_of(&p);
        assert_eq!(op["trim"]["top"], "10p");
        assert_eq!(op["trim"]["left"], 0.1);
        // dpr is capped at 2 upstream.
        assert!(parse_options("dpr=3", &HeaderMap::new(), "example.com").is_err());
        assert!(parse_options("dpr=2", &HeaderMap::new(), "example.com").is_ok());
    }

    #[test]
    fn looks_like_svg_sniffs_markup_not_magic() {
        assert!(looks_like_svg(b"<svg xmlns=\"x\"/>"));
        assert!(looks_like_svg(b"  \n<svg"));
        assert!(looks_like_svg(b"<?xml version=\"1.0\"?><svg/>"));
        // UTF-16 BOMs — xml-rs decodes them, so the gate must too.
        assert!(looks_like_svg(b"\xFF\xFE<\x00s\x00v\x00g\x00"));
        assert!(looks_like_svg(b"\xFE\xFF\x00<\x00?\x00x\x00m\x00l"));
        assert!(!looks_like_svg(b"\x89PNG\r\n\x1a\n"));
        assert!(!looks_like_svg(b"<html><svg/></html>")); // no leading <
        assert!(!looks_like_svg(b"random text"));
    }

    #[test]
    fn sanitize_svg_strips_scripts_and_event_handlers() {
        let out = sanitize_svg(
            br#"<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script><rect width="4" onload="x()"/></svg>"#,
        )
        .unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(!s.contains("script"), "{s}");
        assert!(!s.contains("onload"), "{s}");
        assert!(s.contains("<rect"), "{s}");
    }

    #[test]
    fn sanitize_svg_relativizes_urls_and_filters_data() {
        let out = sanitize_svg(
            br##"<svg xmlns="http://www.w3.org/2000/svg"><feImage href="https://e.com/x.png"/><feImage href="data:text/html,xx"/><feImage href="data:image/png;base64,AAAA"/></svg>"##,
        )
        .unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("href=\"/x.png\""), "{s}");
        assert!(s.contains("href=\"data:image/png;base64,AAAA\""), "{s}");
        assert!(!s.contains("data:text/html"), "{s}");
    }

    #[test]
    fn sanitize_svg_rejects_malformed_and_non_svg() {
        assert!(sanitize_svg(b"<svg xmlns=\"x\"><rect").is_err());
        assert!(sanitize_svg(b"<html><body>x</body></html>").is_err());
        // The sniff gate can pass a non-SVG XML doc — sanitize must
        // still fail closed (no acceptable root element).
        assert!(sanitize_svg(b"<?xml version=\"1.0\"?><html/>").is_err());
    }

    fn ops_of(p: &Parsed) -> serde_json::Value {
        serde_json::from_str(&p.transforms).unwrap()
    }

    #[test]
    fn draw_entry_maps_to_paired_ops() {
        let p = opts("width=800,draw=((url='/logo.png',bottom=5,right=5,fit=contain,width=100,opacity=0.8),(url='/wm.png',repeat=x))");
        assert_eq!(p.draws.len(), 2);
        assert_eq!(p.draws[0].path, "/logo.png");
        assert_eq!(p.draws[1].path, "/wm.png");
        let ops = ops_of(&p);
        assert_eq!(ops.as_array().unwrap().len(), 4);
        assert_eq!(ops[0], serde_json::json!({"width": 800}));
        // Overlay transform op carries imageIndex + percentBasis.
        assert_eq!(
            ops[1],
            serde_json::json!({"imageIndex": 1, "percentBasis": 0, "fit": "contain", "width": 100.0})
        );
        assert_eq!(
            ops[2],
            serde_json::json!({"drawImageIndex": 1, "targetImageIndex": 0, "bottom": 5.0, "right": 5.0, "opacity": 0.8})
        );
        // No transform keys → draw op only.
        assert_eq!(
            ops[3],
            serde_json::json!({"drawImageIndex": 2, "targetImageIndex": 0, "repeat": "x"})
        );
    }

    #[test]
    fn draw_percent_dims_and_dpr_scaling() {
        let p = opts("dpr=2,draw=((url='/l.png',width='25p',height='50p',right=5))");
        let ops = ops_of(&p);
        // "Np" passes through for the facade to resolve against the
        // base canvas; px values and offsets scale with dpr.
        assert_eq!(ops[1]["width"], "25p");
        assert_eq!(ops[1]["height"], "50p");
        assert_eq!(ops[1]["percentBasis"], 0);
        assert_eq!(ops[2]["right"], 10.0);
        // px dims scale too.
        let p2 = opts("dpr=2,draw=((url='/l.png',width=40))");
        assert_eq!(ops_of(&p2)[1]["width"], 80.0);
    }

    #[test]
    fn draw_unwrapped_single_entry_and_url_forms() {
        // Bare `draw=(k=v,...)` reads as one entry.
        let p = opts("draw=(url='/l.png',top=0)");
        assert_eq!(p.draws.len(), 1);
        // Same-origin absolute URL rewrites to its path.
        let p2 = opts("draw=((url='https://example.com/a/b.png'))");
        assert_eq!(p2.draws[0].path, "/a/b.png");
        // Bare relative segment.
        let p3 = opts("draw=((url='l.png'))");
        assert_eq!(p3.draws[0].path, "/l.png");
    }

    #[test]
    fn draw_bad_entries_fail_closed() {
        for (raw, code) in [
            ("draw=((text='HI'))", 9401),           // font stack not vendored
            ("draw=((font=(url='/f.otf')))", 9401), // text-only key, no url
            ("draw=((bottom=5))", 9423),            // url required
            ("draw=((url='/l.png',left=1,right=2))", 9423), // axis conflict
            ("draw=((url='/l.png',top=0,bottom=0))", 9423),
            ("draw=((url='/l.png',bogus=1))", 9423),
            ("draw=((url='/l.png',opacity=1.5))", 9423),
            ("draw=((url='/l.png',composite=screen))", 9423),
            ("draw=((url='/l.png',width=0p))", 9423),
            ("draw=foo", 9423),                              // not a group
            ("draw=((url='https://evil.com/l.png'))", 9401), // foreign host
        ] {
            assert_eq!(
                parse_options(raw, &HeaderMap::new(), "example.com")
                    .err()
                    .unwrap(),
                code,
                "{raw}"
            );
        }
    }

    #[test]
    fn draw_entry_groups_and_gravity_forms() {
        // Structured groups inside entries type like the top-level
        // options — gravity injects mode:"remainder" for the facade.
        let p = opts("draw=((url='/l.png',gravity=(x=0.25,y=0.75),sharpen=(mode=gaussian,amount=2,sigma=1),fit=(mode=crop)))");
        let ops = ops_of(&p);
        assert_eq!(
            ops[1]["gravity"],
            serde_json::json!({"x": 0.25, "y": 0.75, "mode": "remainder"})
        );
        assert_eq!(
            ops[1]["sharpen"],
            serde_json::json!({"mode": "gaussian", "amount": 2.0, "sigma": 1.0})
        );
        assert_eq!(ops[1]["fit"], serde_json::json!({"mode": "crop"}));
        // Bare gravity fractions map the same way.
        let p2 = opts("draw=((url='/l.png',gravity=0.25x0.75))");
        assert_eq!(
            ops_of(&p2)[1]["gravity"],
            serde_json::json!({"x": 0.25, "y": 0.75, "mode": "remainder"})
        );
    }
}
