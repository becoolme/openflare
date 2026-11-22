//! Real-workerd wrapped-binding smoke matrix: `vectorize`, `send_email`,
//! `k2` and `flagship`, one tenant worker per binding.
//!
//! What unit tests cannot prove and this does:
//! - the rendered `wrapped` bindings load under the pinned binary:
//!   `cloudflare-internal:vectorize-api` is the upstream shim and is
//!   asserted strictly; `cloudflare-internal:send-email` /
//!   `cloudflare-internal:k2` / `cloudflare-internal:flagship` are FORK
//!   shims — when the pinned binary predates them the service cannot
//!   instantiate, and those tests SKIP with the workerd log printed
//!   (they turned green when the pin gained the shims);
//! - the provider-backed shims' fetch calls reach facade-vectorize /
//!   facade-email / facade-k2 through the inner `fetcher` service
//!   binding, and each facade forwards to `BINDING_BACKEND` under the
//!   signed-props `bindingId` (`{workerId}~{name}`) at the provider
//!   path `/internal/bindings/v1/<product>/<bindingId>/<leaf>`;
//!   flagship has no backend hop — the app config rides the `config`
//!   innerBinding and evaluates in-isolate;
//! - the typed envelopes round-trip: vectorize `{code,error}` throws
//!   `<OP>_ERROR (code = N)`, send_email `{success,error}` throws an
//!   Error carrying `.code`, k2 `{success:false,error}` resolves
//!   without throwing, flagship throws FlagshipEvaluationError
//!   carrying `.errorCode` — exactly the contracted classes.
//!
//! The `binding-backend` external is replaced by a stub worker that
//! speaks the provider envelope; provider internals are covered by the
//! vrt-provider suite, not this file.
//!
//! Gated on `VRT_WORKERD_BIN` — absent → explicit skip with a printed
//! reason, never silently green:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test bindings_workerd -- --include-ignored

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use common::hyperdrive::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use vrt_config::{
    render_tenant_config, DeployManifest, EmbedKind, SecretValue, TenantRenderInput,
    VersionBundle,
};

/// Tenant worker exercising each provider-backed wrapped binding on a
/// dedicated route (a worker only ever calls the binding it declares).
/// The vectorize route additionally echoes the provider path the
/// backend observed (`matches[0].id` smuggles it back through the typed
/// envelope), proving the facade's `bindingId` routing end to end.
const USER_MODULE: &str = r#"export default {
  async fetch(req, env) {
    const p = new URL(req.url).pathname;
    try {
      if (p === "/vectorize") {
        const ins = await env.VEC.insert([
          { id: "a", values: [1, 0, 0, 0], metadata: { t: "x" }, namespace: "n1" },
        ]);
        const q = await env.VEC.query([1, 0, 0, 0], { topK: 1 });
        const info = await env.VEC.describe();
        const got = await env.VEC.getByIds(["a"]);
        let del = "no-error";
        try {
          await env.VEC.deleteByIds(["a"]);
        } catch (e) {
          del = "err:" + e.message;
        }
        return Response.json({
          mutationId: ins.mutationId,
          count: q.count,
          seen: q.matches[0].id,
          dims: info.dimensions,
          vectorCount: info.vectorCount,
          gotId: got[0].id,
          del,
        });
      }
      if (p === "/email") {
        const ok = await env.EMAIL.send({
          from: "hello@example.com", to: "user@example.com",
          subject: "hi", text: "hello",
        });
        let denied = "no-error";
        try {
          await env.EMAIL.send({
            from: "hello@example.com", to: "user@example.com",
            subject: "hi", text: "hello",
            headers: { "x-mock": "denied" },
          });
        } catch (e) {
          denied = "err:" + e.code + ":" + String(e.message).slice(0, 40);
        }
        return Response.json({ messageId: ok.messageId, denied });
      }
      if (p === "/k2") {
        const ok = await env.K2.send([{ content: "hello" }]);
        const fail = await env.K2.send([
          { content: "x", headers: { "x-mock": "fail" } },
        ]);
        return Response.json({
          ok: ok.success,
          failCode: fail.error && fail.error.code,
          retryable: fail.error && fail.error.retryable,
        });
      }
      if (p === "/flagship") {
        const pro = await env.FLAGS.getBooleanValue("dark-mode", false, { plan: "pro" });
        const free = await env.FLAGS.getBooleanValue("dark-mode", true, { plan: "free" });
        const det = await env.FLAGS.getStringDetails("banner", "d?");
        let missing = "no-error";
        try {
          await env.FLAGS.get("no-such-flag");
        } catch (e) {
          missing = "err:" + (e.errorCode || e.name);
        }
        return Response.json({
          pro, free,
          banner: det.value, variant: det.variant, reason: det.reason,
          missing,
        });
      }
      if (url.pathname === "/secrets") {
        const value = await env.SECRET.get();
        let missing = "no-error";
        try {
          await env.OTHER.get();
        } catch (e) {
          missing = "err:" + e.message;
        }
        return Response.json({ value, missing });
      }
      if (url.pathname === "/versionmeta") {
        const m = env.CF_VERSION_METADATA;
        return Response.json({ id: m.id, tag: m.tag, timestamp: m.timestamp });
      }
      return new Response("ok");
    } catch (e) {
      return new Response("unexpected:" + String((e && e.stack) || e), { status: 500 });
    }
  },
};
"#;

/// Stub `binding-backend` worker speaking the provider envelope for
/// the three products. The vectorize `query` reply smuggles the
/// observed path back so the test can assert the facade's
/// `{bindingId}` routing — `matches[0].id` is provider data the shim
/// passes through untouched.
const BACKEND_STUB: &str = r#"export default {
  async fetch(request) {
    const url = new URL(request.url);
    const p = url.pathname;
    const leaf = p.split("/").filter(Boolean).pop() ?? "";
    if (p.startsWith("/internal/bindings/v1/vectorize/")) {
      if (leaf === "info") {
        return Response.json({
          vectorCount: 1, dimensions: 4,
          processedUpToDatetime: 1700000000000,
          processedUpToMutation: "m-1",
        });
      }
      if (leaf === "query") {
        return Response.json({ count: 1, matches: [
          { id: "seen:" + p, score: 0.99, namespace: "n1" }] });
      }
      if (leaf === "insert" || leaf === "upsert") {
        return Response.json({ mutationId: "m-1" });
      }
      if (leaf === "getByIds") {
        return Response.json([{ id: "a", values: [1, 0, 0, 0], metadata: { t: "x" } }]);
      }
      if (leaf === "deleteByIds") {
        return Response.json({ code: 3101, error: "denied" }, { status: 403 });
      }
      return Response.json({ code: 3000, error: "bad leaf" }, { status: 404 });
    }
    if (p.startsWith("/internal/bindings/v1/email/")) {
      const body = await request.json().catch(() => null);
      const marker = body && body.headers && body.headers["x-mock"];
      if (marker === "denied") {
        return Response.json(
          { success: false, error: { code: "E_SENDER_NOT_VERIFIED",
              message: "sender not allowed", retryable: false } },
          { status: 403 });
      }
      return Response.json({ success: true, messageId: "msg-smoke-1" });
    }
    if (p.startsWith("/internal/bindings/v1/k2/")) {
      const body = await request.json().catch(() => null);
      const marker = body && body.records && body.records[0]
        && body.records[0].headers && body.records[0].headers["x-mock"];
      if (marker === "fail") {
        return Response.json(
          { success: false, error: { code: "SLOTS_FULL",
              message: "no lease", retryable: true } });
      }
      return Response.json({ success: true });
    }
    return Response.json({ error: "bad route" }, { status: 404 });
  }
}
"#;

/// Render the tenant capnp into `dir` for ONE worker carrying
/// `bindings` (plus optional `flagship_apps` tenant resources),
/// materialize every embed, retarget the worker socket onto `port`
/// and — when the render emitted one — replace the `binding-backend`
/// external with the provider-protocol stub worker. `needles` are the
/// structural substrings the emitted text must contain for this
/// binding set.
fn render_and_write(
    dir: &Path,
    port: u16,
    bindings: serde_json::Value,
    flagship_apps: serde_json::Value,
    secrets: &[(&str, &str)],
    needles: &[&str],
) -> PathBuf {
    let worker_id = "wsmoke";
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": USER_MODULE },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-smoke",
        "workers": [{
            "workerId": worker_id,
            "name": "smoke",
            "activeVersion": {
                "versionId": "v-smoke-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": [],
                "tag": "smoke-tag",
                "createdAt": "2026-11-23T01:02:03Z"
            },
            "bindings": bindings,
            "routes": [{"hostname": "smoke--team-smoke.vibeploy.app"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }],
        "flagshipApps": flagship_apps
    }))
    .unwrap();
    let mut params = smoke_params();
    {
        let t = params.secrets.entry("team-smoke".to_string()).or_default();
        for (r, v) in secrets {
            t.insert((*r).to_string(), SecretValue::new(*v));
        }
    }
    let input = TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();
    for needle in needles {
        assert!(
            out.text.contains(needle),
            "render must contain {needle}:\n{}",
            out.text
        );
    }

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
                std::fs::write(&dst, USER_MODULE).unwrap();
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
    // `limits` is not under test; strip so an unpinned local workerd
    // still exercises the binding paths (CI's pinned binary parses it).
    text = strip_field(&text, "limits");
    let external = "(name = \"binding-backend\", external = (http = ()))";
    if text.contains(external) {
        text = text.replace(
            external,
            "(name = \"binding-backend\", worker = (\n            \
               modules = [(name = \"backend.js\", \
               esModule = embed \"backend-stub.js\")],\n            \
               compatibilityDate = \"2026-09-08\",\n            \
               globalOutbound = (name = \"vrt-no-network\", \
               entrypoint = \"DenyAll\"),\n        ))",
        );
        std::fs::write(dir.join("backend-stub.js"), BACKEND_STUB).unwrap();
    }

    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    path
}

/// Remove `name = (…)` capnp field spans (balanced parens) from the
/// rendered text — drops fields an unpinned local binary cannot parse.
fn strip_field(text: &str, name: &str) -> String {
    let needle = format!("{name} = (");
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find(&needle) {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + needle.len()..];
        let mut depth = 1usize;
        let mut end = 0usize;
        for (i, c) in after.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        assert!(end > 0, "unbalanced parens after {name}");
        rest = &after[end..];
        // Removing a field must also remove one separator or the capnp
        // list gains an empty item: eat the following ',' when there is
        // a next field; when the removed field was last (next non-ws
        // closes the parent), eat the preceding ',' instead.
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

/// GET `path`, tolerating a hard service failure: `None` when the
/// socket resets — the wrapped binding could not instantiate (on pins
/// that predate the fork shims, send_email/k2 hit exactly this).
fn try_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(15))).ok()?;
    write!(
        s,
        "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        path
    )
    .ok()?;
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
            Err(_) => return None,
        }
    }
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Some((status, body))
}

/// Printed once per skipped fork-shim test — the workerd log carries
/// the real "cannot serve" reason for CI forensics.
fn skip_fork_shim(proc: &WorkerProc, what: &str) {
    eprintln!(
        "SKIP: pinned workerd cannot serve {what} — it predates the \
         fork shim release (cloudflare-internal:send-email / k2 land \
         after pin vrt-20261120-e699fea). Worker log follows:\n{}",
        log_text(proc)
    );
}

/// `env.VEC` round-trips through the upstream vectorize-api shim:
/// insert/describe/getByIds return typed envelopes and `deleteByIds`
/// throws `VECTOR_DELETE_ERROR (code = 3101)` — the shim's contracted
/// non-200 `{code,error}` surface. `seen` echoes the provider path the
/// facade computed, proving `bindingId` routing end to end.
#[test]
#[ignore = "needs VRT_WORKERD_BIN"]
fn vectorize_binding_traverses_shim_end_to_end() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real workerd required");
        return;
    };
    let Some(dir) = smoke_root("vrt-bindings-smoke") else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(
        &dir,
        port,
        json!([{
            "type": "vectorize", "name": "VEC",
            "indexId": "abcdefghijklmnopqrstuvwxyz",
            "indexName": "smoke-index", "dimensions": 4,
            "metric": "cosine",
            "permissions": {"read": true, "write": true}
        }]),
        json!([]),
        &[],
        &[
            "moduleName = \"cloudflare-internal:vectorize-api\"",
            "(name = \"indexId\", text = \"abcdefghijklmnopqrstuvwxyz\")",
            "(name = \"indexVersion\", text = \"v2\")",
        ],
    );
    let proc = spawn(&bin, &cfg, &dir);
    assert!(wait_ready(port), "workerd did not start:\n{}", log_text(&proc));

    let v = http_json(port, "/vectorize");
    assert_eq!(v["mutationId"], "m-1", "insert: {v}");
    assert_eq!(v["count"], 1, "query count: {v}");
    assert_eq!(
        v["seen"],
        "seen:/internal/bindings/v1/vectorize/wsmoke~VEC/query",
        "facade must route under the signed bindingId: {v}"
    );
    assert_eq!(v["dims"], 4, "describe dimensions: {v}");
    assert_eq!(v["vectorCount"], 1, "describe vectorCount: {v}");
    assert_eq!(v["gotId"], "a", "getByIds: {v}");
    assert!(
        v["del"]
            .as_str()
            .unwrap_or_default()
            .contains("VECTOR_DELETE_ERROR (code = 3101)"),
        "deleteByIds must surface the typed code: {v}"
    );
}

/// `env.EMAIL` resolves `{messageId}` on success and throws an Error
/// carrying `.code` (never a TypeError) on the typed failure envelope —
/// the send-email shim's contracted surface. Skips when the pinned
/// binary predates the fork shim (see module doc).
#[test]
#[ignore = "needs VRT_WORKERD_BIN"]
fn send_email_binding_traverses_shim_end_to_end() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real workerd required");
        return;
    };
    let Some(dir) = smoke_root("vrt-bindings-smoke") else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(
        &dir,
        port,
        json!([{
            "type": "sendEmail", "name": "EMAIL",
            "permissions": {"read": false, "write": true}
        }]),
        json!([]),
        &[],
        &["moduleName = \"cloudflare-internal:send-email\""],
    );
    let proc = spawn(&bin, &cfg, &dir);
    assert!(wait_ready(port), "workerd did not start:\n{}", log_text(&proc));

    let Some((st, body)) = try_get(port, "/email") else {
        skip_fork_shim(&proc, "cloudflare-internal:send-email");
        return;
    };
    assert_eq!(st, 200, "email route: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["messageId"], "msg-smoke-1", "send: {v}");
    assert!(
        v["denied"]
            .as_str()
            .unwrap_or_default()
            .starts_with("err:E_SENDER_NOT_VERIFIED"),
        "denied send must surface the typed code: {v}"
    );
}

/// `env.K2.send` resolves `{success:true}` on commit and the
/// non-throwing `{success:false,error:{code,retryable}}` on the typed
/// failure envelope — the k2 shim's contracted surface. Skips when the
/// pinned binary predates the fork shim (see module doc).
#[test]
#[ignore = "needs VRT_WORKERD_BIN"]
fn k2_binding_traverses_shim_end_to_end() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real workerd required");
        return;
    };
    let Some(dir) = smoke_root("vrt-bindings-smoke") else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(
        &dir,
        port,
        json!([{
            "type": "k2", "name": "K2",
            "streamId": "0123456789abcdef0123456789abcdef",
            "permissions": {"read": false, "write": true}
        }]),
        json!([]),
        &[],
        &["moduleName = \"cloudflare-internal:k2\""],
    );
    let proc = spawn(&bin, &cfg, &dir);
    assert!(wait_ready(port), "workerd did not start:\n{}", log_text(&proc));

    let Some((st, body)) = try_get(port, "/k2") else {
        skip_fork_shim(&proc, "cloudflare-internal:k2");
        return;
    };
    assert_eq!(st, 200, "k2 route: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["ok"], true, "send commit: {v}");
    assert_eq!(v["failCode"], "SLOTS_FULL", "typed failure code: {v}");
    assert_eq!(v["retryable"], true, "typed failure retryable: {v}");
}

/// `env.FLAGS` evaluates inside the isolate through the fork flagship
/// shim: a targeting rule serves `TARGETING_MATCH`, a rule-less flag
/// serves `STATIC`, and `get()` without a default throws
/// `FlagshipEvaluationError` carrying `errorCode = FLAG_NOT_FOUND` —
/// the shim's contracted classes. There is no backend hop by design:
/// the app config rides the `config` innerBinding verbatim.
#[test]
#[ignore = "needs VRT_WORKERD_BIN"]
fn flagship_binding_evaluates_in_isolate() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real workerd required");
        return;
    };
    let Some(dir) = smoke_root("vrt-bindings-smoke") else {
        return;
    };
    let port = free_port();
    let app_config = json!({
        "appId": "app-smoke",
        "accountTag": "acct-1",
        "flags": {
            "dark-mode": {
                "enabled": true,
                "type": "boolean",
                "defaultVariant": "off",
                "variants": {"on": true, "off": false},
                "rules": [{
                    "conditions": [
                        {"attribute": "plan", "operator": "equals", "value": "pro"}
                    ],
                    "serve_variation": "on"
                }]
            },
            "banner": {
                "enabled": true,
                "type": "string",
                "defaultVariant": "plain",
                "variants": {"plain": "hello", "fancy": "HELLO!"},
                "rules": []
            }
        }
    })
    .to_string();
    let cfg = render_and_write(
        &dir,
        port,
        json!([{
            "type": "flagship", "name": "FLAGS", "appId": "app-smoke"
        }]),
        json!([{"appId": "app-smoke", "config": app_config}]),
        &[],
        &[
            "moduleName = \"cloudflare-internal:flagship\"",
            "name = \"config\"",
            "dark-mode",
        ],
    );
    let proc = spawn(&bin, &cfg, &dir);
    assert!(wait_ready(port), "workerd did not start:\n{}", log_text(&proc));

    let Some((st, body)) = try_get(port, "/flagship") else {
        skip_fork_shim(&proc, "cloudflare-internal:flagship");
        return;
    };
    assert_eq!(st, 200, "flagship route: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["pro"], true, "targeting rule must serve on=true: {v}");
    assert_eq!(v["free"], false, "no rule match must serve default off=false: {v}");
    assert_eq!(v["banner"], "hello", "static flag value: {v}");
    assert_eq!(v["variant"], "plain", "static flag variant: {v}");
    assert_eq!(v["reason"], "STATIC", "rule-less flag reason: {v}");
    assert_eq!(
        v["missing"], "err:FLAG_NOT_FOUND",
        "get() without default must surface the typed code: {v}"
    );
}

/// `env.SECRET.get()` traverses the bundled `/workerd/secrets-store.js`
/// worker: the tenant binding is a service binding carrying the CF
/// `{store_id, secret_name}` props and the `SecretsStoreSecret`
/// entrypoint; the VALUE arrives through the service worker's
/// `env:<secret_name>` `fromEnvironment` binding (never the config).
/// `OTHER` declares a name whose `env:` variable the launch env does
/// not carry — `get()` surfaces upstream's `Secret "<name>" not found`.
/// Bundled modules ship in the pinned binary, so this asserts strictly
/// (no fork shim involved).
#[test]
#[ignore = "needs VRT_WORKERD_BIN"]
fn secrets_store_binding_serves_env_backed_value() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real workerd required");
        return;
    };
    let Some(dir) = smoke_root("vrt-bindings-smoke") else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(
        &dir,
        port,
        json!([
            {
                "type": "secretsStore", "name": "SECRET",
                "storeId": "default", "secretName": "api-key",
                "secretRef": "secrets-store:default:api-key"
            },
            {
                "type": "secretsStore", "name": "OTHER",
                "storeId": "default", "secretName": "other-key",
                "secretRef": "secrets-store:default:other-key"
            }
        ]),
        json!([]),
        &[
            ("secrets-store:default:api-key", "hunter42"),
            ("secrets-store:default:other-key", "other-value"),
        ],
        &[
            "bundled = \"/workerd/secrets-store.js\"",
            "entrypoint = \"SecretsStoreSecret\"",
            "name = \"env:api-key\"",
            "name = \"env:other-key\"",
        ],
    );
    // `env:api-key` resolves against the process env — inject only
    // SECRET's; `env:other-key` stays unset to prove the not-found path.
    std::env::set_var(
        vrt_config::secret_env_name("wsmoke", "secrets-store:api-key"),
        "hunter42",
    );
    let proc = spawn(&bin, &cfg, &dir);
    assert!(wait_ready(port), "workerd did not start:\n{}", log_text(&proc));

    let v = http_json(port, "/secrets");
    assert_eq!(v["value"], "hunter42", "get() resolves the env value: {v}");
    assert_eq!(
        v["missing"], "err:Secret \"other-key\" not found",
        "unset env var must surface upstream's not-found: {v}"
    );
}

/// `env.CF_VERSION_METADATA` is a plain `json` binding materialized
/// from the active version: `{id, tag, timestamp}` per CF's
/// `WorkerVersionMetadata`. The render helper stamps
/// `tag = "smoke-tag"` / `createdAt = "2026-11-23T01:02:03Z"` on the
/// version, so the binding must echo exactly those.
#[test]
#[ignore = "needs VRT_WORKERD_BIN"]
fn version_metadata_binding_serves_active_version_json() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real workerd required");
        return;
    };
    let Some(dir) = smoke_root("vrt-bindings-smoke") else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(
        &dir,
        port,
        json!([{ "type": "versionMetadata", "name": "CF_VERSION_METADATA" }]),
        json!([]),
        &[],
        &["json = \"{\\\"id\\\":\\\"v-smoke-1\\\",\\\"tag\\\":\\\"smoke-tag\\\",\\\"timestamp\\\":\\\"2026-11-23T01:02:03Z\\\"}\""],
    );
    let proc = spawn(&bin, &cfg, &dir);
    assert!(wait_ready(port), "workerd did not start:\n{}", log_text(&proc));

    let v = http_json(port, "/versionmeta");
    assert_eq!(
        v,
        json!({ "id": "v-smoke-1", "tag": "smoke-tag",
                "timestamp": "2026-11-23T01:02:03Z" }),
        "version metadata must mirror the active version"
    );
}
