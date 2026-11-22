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
use axum::body::Bytes;
use axum::http::{header, HeaderMap, Request, Response, StatusCode};
use axum::body::Body;
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
    let parsed = match parse_options(opts_raw, headers) {
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
    metadata: bool,
    onerror_redirect: bool,
}

fn parse_num(s: &str, min: f64, max: f64) -> Result<f64, u32> {
    let v: f64 = s.parse().map_err(|_| 9423u32)?;
    if !v.is_finite() || v < min || v > max {
        return Err(9423);
    }
    Ok(v)
}

/// Translate `k=v,k=v` URL options into the facade `transforms` op JSON.
/// CF's URL vocabulary is a near-1:1 projection of the binding op keys;
/// keys with no vendored-engine support pass through so the facade
/// fails closed with the typed code (`segment`, `zoom`, `anim`→9401).
fn parse_options(raw: &str, headers: &HeaderMap) -> Result<Parsed, u32> {
    let mut op = serde_json::Map::new();
    let mut p = Parsed {
        transforms: String::new(),
        output_format: None,
        output_quality: None,
        anim: false,
        metadata: false,
        onerror_redirect: false,
    };
    let mut dpr = 1.0f64;
    let mut width: Option<f64> = None;
    let mut height: Option<f64> = None;
    for pair in raw.split(',') {
        let (key, value) = pair.split_once('=').ok_or(9423u32)?;
        let v = value.trim();
        match key {
            "width" => width = Some(parse_num(v, 1.0, 16384.0)?),
            "height" => height = Some(parse_num(v, 1.0, 16384.0)?),
            "dpr" => dpr = parse_num(v, 0.1, 4.0)?,
            "fit" => match v {
                "scale-down" | "contain" | "cover" | "crop" | "pad" | "squeeze" => {
                    op.insert("fit".into(), v.into());
                }
                _ => return Err(9423),
            },
            "gravity" => {
                // Named anchors pass through; `XxY` fractions map onto
                // the binding's coordinate-gravity object.
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
            "rotate" => {
                let d = parse_num(v, 0.0, 360.0)?;
                op.insert("rotate".into(), (d as u32).into());
            }
            "blur" => {
                op.insert("blur".into(), parse_num(v, 1.0, 250.0)?.into());
            }
            "sharpen" => {
                op.insert("sharpen".into(), parse_num(v, 0.0, 10.0)?.into());
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
            "trim" => {
                // `top x right x bottom x left` pixel inset, or `border`
                // for content-detected trimming.
                if v == "border" {
                    op.insert("trim".into(), "border".into());
                } else {
                    let nums: Vec<&str> = v.split('x').collect();
                    if nums.len() != 4 {
                        return Err(9423);
                    }
                    let mut m = serde_json::Map::new();
                    for (k, s) in ["top", "right", "bottom", "left"].iter().zip(nums) {
                        m.insert((*k).into(), parse_num(s, 0.0, 16384.0)?.into());
                    }
                    op.insert("trim".into(), m.into());
                }
            }
            "border" => {
                // `<width>x<color>` (uniform) — per-side borders are a
                // binding-only shape, not part of the URL vocabulary.
                let (w, c) = v.split_once('x').ok_or(9423u32)?;
                let w = parse_num(w, 0.0, 4096.0)?;
                if c.len() > 32 {
                    return Err(9423);
                }
                op.insert(
                    "border".into(),
                    serde_json::json!({"width": w, "color": format!("#{c}")}),
                );
            }
            "padding" => {
                // Uniform or `topxrightxbottomxleft`; painted with the
                // `background` color (or transparent).
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
                op.insert(
                    "border".into(),
                    serde_json::json!({"top": t, "right": r, "bottom": b, "left": l}),
                );
            }
            "format" => match v {
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
            "quality" => {
                p.output_quality = Some(parse_num(v, 1.0, 100.0)? as u32);
            }
            "anim" => {
                p.anim = match v {
                    "true" => true,
                    "false" => false,
                    _ => return Err(9423),
                };
            }
            "metadata" => {
                p.metadata = match v {
                    "true" | "exif" | "copyright" => true,
                    "none" | "false" => false,
                    _ => return Err(9423),
                };
            }
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
            _ => return Err(9423),
        }
    }
    if let Some(w) = width {
        op.insert("width".into(), ((w * dpr).round().max(1.0) as u64).into());
    }
    if let Some(h) = height {
        op.insert("height".into(), ((h * dpr).round().max(1.0) as u64).into());
    }
    p.transforms = serde_json::json!([serde_json::Value::Object(op)]).to_string();
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
    let WorkerEndpoint::Unix(path) = worker_ep else {
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
    let upstream: Result<Response<Body>, u16> =
        tokio::time::timeout(UPSTREAM_TIMEOUT, async {
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

            // 2. Multipart POST to the facade-images socket.
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
            body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

            let ep = images_endpoint(&worker_ep).ok_or(9411u16)?;
            let post = Request::builder()
                .method("POST")
                .uri(if parsed.metadata { "/info" } else { "/transform" })
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
                StatusCode::from_u16(status.clamp(400, 599))
                    .unwrap_or(StatusCode::BAD_GATEWAY),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(raw: &str) -> Parsed {
        parse_options(raw, &HeaderMap::new()).expect(raw)
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
        for raw in ["width=0", "width=", "bogus=1", "fit=stretch", "quality=101", "rotate=abc"] {
            assert!(parse_options(raw, &HeaderMap::new()).is_err(), "{raw}");
        }
    }

    #[test]
    fn format_auto_negotiates_on_accept() {
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT, "image/avif,image/webp".parse().unwrap());
        let p = parse_options("format=auto", &h).unwrap();
        assert_eq!(p.output_format.as_deref(), Some("image/avif"));
        let p = parse_options("format=auto", &HeaderMap::new()).unwrap();
        assert_eq!(p.output_format.as_deref(), Some("image/jpeg"));
    }
}
