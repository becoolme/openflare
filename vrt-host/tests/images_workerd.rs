//! Real-workerd Images binding smoke test.
//!
//! What unit tests cannot prove and this does:
//! - the rendered `wrapped` binding over `cloudflare-internal:images-api`
//!   loads under the pinned binary (upstream shim, unmodified);
//! - `facade-images`' `wasm = embed` system modules compile inside
//!   workerd — the vendored jSquash emscripten/wasm-bindgen glue
//!   instantiates through the same `WebAssembly.Module` the `.wasm`
//!   module type hands importers;
//! - a tenant `env.IMAGES.info()`/`input().transform().output()` call
//!   round-trips through the real fetcher service binding and returns
//!   real pixels.
//!
//! Gated on `VRT_WORKERD_BIN` — absent → explicit skip with a printed
//! reason, never silently green:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test images_workerd -- --include-ignored

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry, EgressPolicy,
    EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};

fn workerd_bin() -> Option<PathBuf> {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) if !p.is_empty() => Some(PathBuf::from(p)),
        _ => None,
    }
}

fn facade_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn smoke_root() -> Option<PathBuf> {
    match vrt_securefs::test_short_root("vrt-images-smoke") {
        Ok(d) => Some(d),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
            None
        }
        Err(e) => panic!("short root: {e}"),
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct WorkerProc {
    child: Child,
    log_path: PathBuf,
    dir: PathBuf,
}

impl Drop for WorkerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::env::var("VRT_SMOKE_KEEP").is_ok() {
            eprintln!("kept smoke dir: {}", self.dir.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn spawn(bin: &Path, cfg: &Path, dir: &Path) -> WorkerProc {
    let log_path = dir.join("workerd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let child = Command::new(bin)
        .arg("serve")
        .arg(cfg)
        .arg("--experimental")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap();
    WorkerProc {
        child,
        log_path,
        dir: dir.to_path_buf(),
    }
}

fn wait_ready(port: u16) -> bool {
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    false
}

fn log_text(proc: &WorkerProc) -> String {
    let raw = std::fs::read(&proc.log_path).unwrap_or_default();
    String::from_utf8_lossy(&raw).replace('\0', "")
}

fn http_get(port: u16, path: &str) -> (u16, Vec<u8>) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(15)))
        .unwrap();
    write!(
        s,
        "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        path
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if response_complete(&buf) {
                    break;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => panic!("read: {e}"),
        }
    }
    let status: u16 = String::from_utf8_lossy(&buf)
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let hdr_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(buf.len());
    (status, buf[hdr_end..].to_vec())
}

fn response_complete(buf: &[u8]) -> bool {
    let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4) else {
        return false;
    };
    let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_lowercase();
    let body_len = buf.len() - hdr_end;
    for line in headers.split("\r\n") {
        if let Some(v) = line.strip_prefix("content-length:") {
            return v.trim().parse::<usize>().map_or(false, |n| body_len >= n);
        }
        if line.starts_with("transfer-encoding:") && line.contains("chunked") {
            return buf[hdr_end..].windows(5).any(|w| w == b"0\r\n\r\n");
        }
    }
    false
}

/// Engine-produced 4×4 RGBA(200,30,60) PNG (facade/images-engine.ts,
/// mozjpeg-independent squoosh_png encoder — checked bytes, not a
/// fabricated header).
const PNG_B64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAQAAAAECAYAAACp8Z5+AAAAT0lEQVR4AQFEALv/\
AMgePP/IHjz/yB48/8gePP8AyB48/8gePP/IHjz/yB48/wDIHjz/yB48/8gePP/IHjz/\
AMgePP/IHjz/yB48/8gePP+AiCIRPzsCuQAAAABJRU5ErkJggg==";

/// Tenant worker: exercises `info()` plus a resize→webp transform.
/// `atob` keeps the fixture one constant — workerd exposes it on the
/// global like every other Workers runtime.
const USER_MODULE: &str = r#"const PNG_B64 = "<PNG_B64>";
function pngBytes() {
  const bin = atob(PNG_B64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}
export default {
  async fetch(req, env) {
    const p = new URL(req.url).pathname;
    try {
      if (p === "/info") {
        const info = await env.IMAGES.info(
          new Response(pngBytes()).body);
        return Response.json(info);
      }
      if (p === "/transform") {
        const out = await env.IMAGES.input(new Response(pngBytes()).body)
          .transform({ width: 2, height: 2, fit: "squeeze" })
          .output({ format: "image/webp", quality: 80 });
        return out.response();
      }
      if (p === "/gif-input") {
        // A truncated GIF header sniffs as gif but carries no frame —
        // must surface the typed corrupt-data error, not a silent
        // passthrough or an opaque 500.
        const gif = new Uint8Array([0x47,0x49,0x46,0x38,0x39,0x61,1,0,1,0]);
        try {
          const r = await env.IMAGES.input(new Response(gif).body)
            .transform({}).output({ format: "image/png" });
          return new Response("no-error:" + r.contentType());
        } catch (e) {
          return new Response("err:" + e.code + ":" + String(e.message).slice(0, 60));
        }
      }
      if (p === "/avif") {
        const out = await env.IMAGES.input(new Response(pngBytes()).body)
          .transform({}).output({ format: "image/avif" });
        return out.response();
      }
      if (p === "/text") {
        // .text() serializes to a `text_input` spec whose font.url the
        // vrt-egress relay must evaluate — a private literal denies at
        // the policy layer before any dial, deterministically surfacing
        // the typed 9420 through ImagesError.code. Proves the shim's
        // text_input emission reaches the relay under real workerd.
        try {
          const r = await env.IMAGES.text("HI", {
            font: { url: "http://10.0.0.1/font.ttf" },
            size: 20,
          }).output({ format: "image/png" });
          return new Response("no-error:" + r.contentType());
        } catch (e) {
          return new Response("err:" + e.code + ":" + String(e.message).slice(0, 60));
        }
      }
      return new Response("ok");
    } catch (e) {
      return new Response("unexpected:" + String(e && e.stack || e), { status: 500 });
    }
  },
};
"#;

fn smoke_params() -> RuntimeParams {
    let raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = parse_facade_manifest(&raw).expect("facade manifest parses");
    RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: Vec::<EgressAllowEntry>::new(),
            deny_names: Vec::new(),
            request_timeout_ms: 30_000,
            ..Default::default()
        },
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        disk_services: vrt_config::resolve_disk_paths(&facade_root(), &manifest.disk_services).unwrap(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: std::collections::BTreeMap::new(),
        secrets: std::collections::BTreeMap::new(),
        do_storage_dir: String::new(),
        wfop_pepper: "images-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

/// Render the real config for one worker carrying ONLY an images
/// binding, materialize every embed (including the `.wasm` system
/// modules — this is where `wasm = embed` proves out), and rewrite the
/// environment-specific bits.
fn render_and_write(dir: &Path, port: u16) -> PathBuf {
    let worker_id = "wimages";
    let user_module = USER_MODULE.replace("<PNG_B64>", PNG_B64);
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": user_module },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-images",
        "workers": [{
            "workerId": worker_id,
            "name": "images",
            "activeVersion": {
                "versionId": "v-images-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [{"type": "images", "name": "IMAGES"}],
            "routes": [{"hostname": "images--team-images.vibeploy.app"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap();
    let params = smoke_params();
    let input = TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();

    // Structural assertions on the REAL emitted text — the wrapped
    // shim module name and the wasm system-module form.
    assert!(
        out.text
            .contains("moduleName = \"cloudflare-internal:images-api\""),
        "images binding must wrap the upstream shim:\n{}",
        out.text
    );
    assert!(
        out.text.contains("wasm = embed"),
        "facade-images must embed .wasm system modules:\n{}",
        out.text
    );

    let manifest_raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest_doc: serde_json::Value = serde_json::from_str(&manifest_raw).unwrap();
    for embed in &out.embeds {
        let dst = dir.join(&embed.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        match &embed.kind {
            EmbedKind::SystemModule {
                service_name,
                module_name,
            } => {
                let modules = manifest_doc["workers"][service_name]["modules"]
                    .as_array()
                    .unwrap_or_else(|| panic!("{service_name} missing from manifest"));
                let m = modules
                    .iter()
                    .find(|m| m["name"].as_str().unwrap() == module_name)
                    .unwrap_or_else(|| panic!("{service_name}/{module_name} not in manifest"));
                let src = facade_root()
                    .join("..")
                    .join(m["dist"].as_str().unwrap());
                let bytes =
                    std::fs::read(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
                let got = format!("{:x}", Sha256::digest(&bytes));
                assert_eq!(
                    got, embed.digest,
                    "embed {service_name}/{module_name} digest drift — \
                     regenerate facade/dist"
                );
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule {
                worker_id: w,
                module_name,
            } => {
                assert_eq!(w, worker_id);
                assert_eq!(module_name, "index.js");
                std::fs::write(&dst, &user_module).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }

    let mut text = out.text.clone();
    let unix_addr = format!("unix:sock/{worker_id}.sock");
    assert!(
        text.contains(&unix_addr),
        "expected unix socket addr in render:\n{text}"
    );
    text = text.replace(&unix_addr, &format!("127.0.0.1:{port}"));
    // The `/cdn-cgi/` loopback socket (`<workerId>-images.sock`) is
    // rendered as a relative unix addr nothing dials in this test —
    // retarget every remaining `unix:sock/*` service addr onto
    // loopback TCP so the pinned binary can bind it.
    while let Some(pos) = text.find("unix:sock/") {
        let end = text[pos..]
            .find('"')
            .map(|i| pos + i)
            .expect("unix addr is quoted");
        let addr = text[pos..end].to_string();
        text = text.replace(&addr, &format!("127.0.0.1:{}", free_port()));
    }
    // The fork's `limits` worker field landed after the oldest binary
    // we smoke against — strip it so an unpinned local workerd still
    // exercises the images path (limits are not under test here).
    text = strip_field(&text, "limits");
    // binding-backend external → inert stub worker (unused on the
    // images path, but the capnp must resolve every referenced name).
    let external = "(name = \"binding-backend\", external = (http = ()))";
    assert!(
        text.contains(external),
        "expected binding-backend external in render:\n{text}"
    );
    text = text.replace(
        external,
        "(name = \"binding-backend\", worker = (\n            \
           modules = [(name = \"backend.js\", \
           esModule = embed \"backend-stub.js\")],\n            \
           compatibilityDate = \"2026-09-08\",\n            \
           globalOutbound = (name = \"vrt-no-network\", \
           entrypoint = \"DenyAll\"),\n        ))",
    );
    std::fs::write(
        dir.join("backend-stub.js"),
        "export default { async fetch() { return new Response(\"stub\"); } }\n",
    )
    .unwrap();

    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    path
}

/// Remove `name = (…)` capnp field spans (balanced parens) from the
/// rendered text — used to drop fields an unpinned local binary cannot
/// parse without weakening what CI's pinned binary exercises… CI runs
/// this same stripped text, which is fine: `limits` is optional.
fn strip_field(text: &str, name: &str) -> String {
    let needle = format!("{name} = (");
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find(&needle) {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + needle.len()..];
        let mut depth = 1usize;
        let mut end = None;
        for (i, c) in after.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let i = end.unwrap_or_else(|| panic!("unbalanced parens after {needle}"));
        rest = &after[i + 1..];
        // Removing a field must also remove one separator or the capnp
        // list gains an empty item: eat the following ',' when there is
        // a next field (whitespace may intervene); when the removed
        // field was last, eat the preceding ',' instead.
        let tail = rest.trim_start();
        if let Some(t) = tail.strip_prefix(',') {
            rest = t;
        } else if out.trim_end().ends_with(',') {
            out.truncate(out.trim_end().len() - 1);
        }
    }
    out.push_str(rest);
    out
}

#[test]
#[ignore = "needs VRT_WORKERD_BIN"]
fn images_binding_info_and_transform_end_to_end() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real workerd required");
        return;
    };
    let Some(dir) = smoke_root() else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(&dir, port);
    let proc = spawn(&bin, &cfg, &dir);
    if !wait_ready(port) {
        panic!(
            "workerd never listened on {port}\n-- workerd.log --\n{}",
            log_text(&proc)
        );
    }

    // info(): pure sniffer path — exact upstream shape.
    let (st, body) = http_get(port, "/info");
    let body = String::from_utf8_lossy(&body);
    assert_eq!(st, 200, "info failed: {body}\n{}", log_text(&proc));
    let info: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        info["format"], "image/png",
        "info format: {info}\n{}",
        log_text(&proc)
    );
    assert_eq!((info["width"].as_u64(), info["height"].as_u64()), (Some(4), Some(4)));

    // transform(): decode → lanczos3 resize → webp encode — the whole
    // WASM pipeline under real workerd.
    let (st, body) = http_get(port, "/transform");
    assert_eq!(
        st,
        200,
        "transform failed: {}\n{}",
        String::from_utf8_lossy(&body),
        log_text(&proc)
    );
    assert!(
        body.len() > 12 && &body[..4] == b"RIFF" && &body[8..12] == b"WEBP",
        "expected webp bytes, got {} bytes head {:?}",
        body.len(),
        &body[..body.len().min(16)]
    );

    // avif encode — the largest vendored wasm (~3.5MB) must
    // instantiate and emit a valid ISO-BMFF container.
    let (st, body) = http_get(port, "/avif");
    assert_eq!(
        st,
        200,
        "avif encode failed: {}\n{}",
        String::from_utf8_lossy(&body),
        log_text(&proc)
    );
    assert!(
        body.len() > 8 && &body[4..8] == b"ftyp",
        "expected avif ftyp box, got {} bytes head {:?}",
        body.len(),
        &body[..body.len().min(16)]
    );

    // Fail-closed edge end-to-end: a truncated GIF surfaces the typed
    // 9420 corrupt-data code through the shim's ImagesError `.code`,
    // not a 500.
    let (st, body) = http_get(port, "/gif-input");
    let body = String::from_utf8_lossy(&body);
    assert_eq!(st, 200, "gif probe failed: {body}");
    assert!(
        body.starts_with("err:9420"),
        "truncated gif must fail closed with 9420: {body}"
    );

    // .text() canvas: the shim's `text_input` spec posts through the
    // fetcher → vrt-egress relay; its private-literal font URL denies
    // at the policy layer (before any dial) → typed 9420.
    let (st, body) = http_get(port, "/text");
    let body = String::from_utf8_lossy(&body);
    assert_eq!(st, 200, "text probe failed: {body}");
    assert!(
        body.starts_with("err:9420"),
        "policy-denied font must fail closed with 9420: {body}"
    );

    let log = log_text(&proc);
    assert!(
        !log.contains("uncaught exception") && !log.contains("Uncaught Exception"),
        "workerd logged an uncaught exception:\n{log}"
    );
    drop(proc);
}

/// cf.image mechanism probe (IMG2): the egress-worker transform relies
/// on workerd serializing `Request.cf` — including arbitrary keys like
/// `image` — into the subrequest channel that `globalOutbound`
/// delivers to the next worker. This minimal two-worker config proves
/// the sink's `request.cf` carries the tenant's cf verbatim; the
/// openflare half (EgressProxy branch + VRT_IMAGES binding + facade
/// transform) is covered by the node harness against the real
/// facade-images handler.
#[test]
#[ignore = "needs VRT_WORKERD_BIN"]
fn cf_blob_reaches_global_outbound() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real workerd required");
        return;
    };
    let Some(dir) = smoke_root() else {
        return;
    };
    let port = free_port();

    let tenant = r#"export default {
  async fetch() {
    try {
      const r = await fetch("http://probe.invalid/img.png", {
        cf: { image: { width: 2, format: "webp" }, marker: "cf-probe" },
      });
      return new Response("status:" + r.status + "|" + (await r.text()));
    } catch (e) {
      return new Response("threw:" + (e && e.message));
    }
  },
};"#;
    let sink = r#"export default {
  async fetch(req) {
    return Response.json(req.cf ?? null);
  },
};"#;
    std::fs::write(dir.join("tenant.js"), tenant).unwrap();
    std::fs::write(dir.join("sink.js"), sink).unwrap();
    let cfg_text = format!(
        r#"using Workerd = import "/workerd/workerd.capnp";
const cfg :Workerd.Config = (
  services = [
    (name = "internet", network = ()),
    (name = "tenant", worker = (
      modules = [ (name = "tenant.js", esModule = embed "tenant.js") ],
      compatibilityDate = "2024-01-01",
      globalOutbound = (name = "sink"),
    )),
    (name = "sink", worker = (
      modules = [ (name = "sink.js", esModule = embed "sink.js") ],
      compatibilityDate = "2024-01-01",
      globalOutbound = (name = "internet"),
    )),
  ],
  sockets = [
    (name = "http", address = "127.0.0.1:{port}", http = (),
     service = (name = "tenant")),
  ],
);"#,
    );
    let cfg = dir.join("cf-probe.capnp");
    std::fs::write(&cfg, cfg_text).unwrap();

    let proc = spawn(&bin, &cfg, &dir);
    if !wait_ready(port) {
        panic!(
            "workerd never listened on {port}\n-- workerd.log --\n{}",
            log_text(&proc)
        );
    }
    let (st, body) = http_get(port, "/");
    let body = String::from_utf8_lossy(&body);
    assert_eq!(st, 200, "probe failed: {body}\n{}", log_text(&proc));
    // The sink must see the whole cf object — `image` included — or the
    // openflare egress can never observe cf.image.
    assert!(
        body.contains("image") && body.contains("marker"),
        "cf blob did not reach the globalOutbound worker: {body}\n{}",
        log_text(&proc)
    );
    drop(proc);
}
