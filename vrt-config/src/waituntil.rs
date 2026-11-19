//! `ctx.waitUntil` accounting (MIG-12) — the platform-trusted pending
//! counter that lets the supervisor defer SIGTERM on a draining
//! generation until its post-response work settles (bounded by
//! `waitUntilGrace`).
//!
//! Stock workerd drops unresolved `waitUntil` promises instantly on
//! SIGTERM, and there is no native "pending waitUntil" query on the
//! process — so the count must come from inside the isolate, from code
//! the tenant cannot touch:
//!
//! * `vrt-wu-prelude.mjs` + `vrt-wu-entry.mjs` are emitted as
//!   `modules[0]`/`modules[1]` of every tenant worker. The entry
//!   module wraps the tenant default export (object → handler Proxy, class →
//!   subclass) so every `fetch`/`scheduled`/`queue` handler runs with
//!   the real ctx whose `waitUntil` is replaced in place and increments a module-scope counter
//!   (decremented when the promise settles). Service-worker-syntax
//!   `addEventListener` registrations are captured by the prelude and
//!   dispatched by the fallback default export with a synthesized
//!   event whose `waitUntil` counts + forwards to the real ctx.
//! * `export class VrtWaituntilProbe extends WorkerEntrypoint` is
//!   added to the same wrapper — a named-entrypoint service binding
//!   (the proven facade-binding mechanism) reaches it. It answers
//!   `{pending, wrapped}` where `wrapped=false` marks a default export
//!   the wrapper could not wrap (fail closed → probe failure → the
//!   supervisor does not trust the count).
//! * `vrt-wu-probe` is a per-generation service worker bound to a
//!   private unix socket (`vrt-wu`) inside `socket_dir` — the
//!   same private-socket model as the heartbeat (F1: socket_dir is
//!   0700, never registered in `worker_endpoints`, unreachable from
//!   tenant ingress). It fans the supervisor's single GET out to every
//!   worker's `VrtWaituntilProbe` and aggregates `{pending, failed}`.
//!   Its `globalOutbound` is deny-all; it carries no tenant bindings,
//!   so tenant code can neither read nor forge another worker's count.
//!
//! Isolation properties (platform-trusted, fail closed):
//! * the counter lives in platform module scope — tenant code cannot
//!   name, import, or shadow it;
//! * a tenant `export class VrtWaituntilProbe` or a bundle module
//!   named `vrt-wu-entry.mjs`/`vrt-wu-prelude.mjs` is a render-time
//!   `ConfigError`, never a silent shadow;
//! * the probe socket is a host-private unix path; tenants get no
//!   binding to `vrt-wu-probe` and no route onto its socket.

/// Service name of the per-generation aggregate probe worker.
pub const PROBE_SERVICE: &str = "vrt-wu-probe";
/// Socket name of the per-generation probe endpoint (in `socket_dir`).
pub const PROBE_SOCKET_NAME: &str = "vrt-wu";
/// Named entrypoint exported by every wrapped tenant worker.
pub const PROBE_ENTRYPOINT: &str = "VrtWaituntilProbe";
/// Wrapper entry module name (modules[0] of every tenant worker).
pub const ENTRY_MODULE: &str = "vrt-wu-entry.mjs";
/// Shared prelude module name (modules[1]).
pub const PRELUDE_MODULE: &str = "vrt-wu-prelude.mjs";
/// Inline probe worker module name.
pub const PROBE_MODULE: &str = "vrt-wu-probe.mjs";
/// Request path the supervisor probes (`GET` on the probe socket).
pub const PROBE_PATH: &str = "/vrt/waituntil";
/// Service-binding names on the probe worker, one per wrapped worker.
pub const PROBE_BINDING_PREFIX: &str = "VRT_WU_";

/// Module-scope counter + ctx/event wrapping + SW-syntax listener
/// capture. Imported by `vrt-wu-entry.mjs` BEFORE the tenant entry so
/// `addEventListener` is already patched when tenant top-level code
/// runs. Shared by every entrypoint class the isolate serves
/// (default, `VrtWaituntilProbe`, DO classes, workflow classes).
pub const PRELUDE_JS: &str = r#"// vrt platform waitUntil accounting (MIG-12). Platform-owned module —
// tenants cannot reach this scope; DO NOT rename bindings.
let __n = 0;
export function __track(p) {
  __n += 1;
  const d = () => { __n -= 1; };
  try { Promise.resolve(p).then(d, d); } catch { d(); }
}
export function __pending() { return __n; }
// In-place wrap: workerd's native base classes (DurableObject,
// WorkerEntrypoint, WorkflowEntrypoint) type-check constructor
// parameter 1, so the REAL ctx object must be passed down — a Proxy
// is rejected. `waitUntil` is replaced on the real object instead.
const __WRAPPED = Symbol("vrt.wu.wrapped");
let __failed = false;
export function __wrapFailed() { return __failed; }
export function __wrapCtx(ctx) {
  if (ctx === null || ctx === undefined || typeof ctx.waitUntil !== "function") {
    return ctx;
  }
  if (ctx.waitUntil[__WRAPPED] === true) return ctx;
  const orig = ctx.waitUntil;
  const w = function (p) { __track(p); return orig.call(ctx, p); };
  w[__WRAPPED] = true;
  try {
    Object.defineProperty(ctx, "waitUntil", {
      value: w,
      configurable: true,
      writable: true,
    });
  } catch {
    __failed = true;
    return ctx;
  }
  // Fail closed: frozen/non-extensible/exotic objects may swallow the
  // write — if the read-back is not ours, accounting is not trusted.
  if (ctx.waitUntil !== w) __failed = true;
  return ctx;
}
// SW-syntax interpose: record fetch/scheduled/queue listener
// registrations so the fallback default export can dispatch to them
// (a module-mode default export shadows addEventListener listeners,
// so the wrapper must invoke them itself).
const __listeners = {};
const __origAdd = globalThis.addEventListener;
if (typeof __origAdd === "function") {
  globalThis.addEventListener = function (type, handler, ...rest) {
    if (type === "fetch" || type === "scheduled" || type === "queue") {
      const orig = typeof handler === "function"
        ? handler
        : (handler !== null && typeof handler === "object" && typeof handler.handleEvent === "function")
          ? (ev) => handler.handleEvent(ev)
          : undefined;
      if (orig !== undefined) (__listeners[type] ??= []).push(orig);
    }
    return __origAdd.call(this, type, handler, ...rest);
  };
}
export function __listenersOf(t) { return __listeners[t] ?? []; }
"#;

fn js_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
}

/// The per-worker wrapper entry module. `main` is the tenant's
/// mainModule name (module specifier `./<main>`); `classes` are the
/// locally-defined DO + workflow entrypoint class names that need a
/// ctx-wrapping subclass re-exported under the same name.
pub fn entry_js(main: &str, classes: &[String]) -> String {
    let spec = format!("./{main}");
    let mut out = String::with_capacity(PRELUDE_JS.len() + 1024);
    out.push_str("// vrt platform waitUntil wrapper (MIG-12) — modules[0] entry.\n");
    out.push_str(&format!(
        "import {{ __track, __pending, __wrapCtx, __wrapFailed, __listenersOf }} from {};\n",
        js_str(&format!("./{PRELUDE_MODULE}"))
    ));
    out.push_str("import { WorkerEntrypoint } from \"cloudflare:workers\";\n");
    out.push_str(&format!("import * as __user from {};\n", js_str(&spec)));
    out.push_str(
        r#"let __wrapped = true;
function __wrapHandler(fn, t) {
  return function (...a) {
    if (a.length >= 3) a[2] = __wrapCtx(a[2]);
    return fn.apply(t, a);
  };
}
function __fallback() {
  // The tenant registered no default export — dispatch to its
  // recorded addEventListener handlers with a synthesized event.
  const fb = {};
  for (const evName of ["fetch", "scheduled", "queue"]) {
    fb[evName] = async function (...a) {
      const ctx = a[a.length - 1];
      const hs = __listenersOf(evName);
      if (!hs.length) {
        return evName === "fetch" ? new Response("no handler", { status: 503 }) : undefined;
      }
      const fake = {
        ...(typeof a[0] === "object" && a[0] !== null ? a[0] : {}),
        waitUntil(p) { __track(p); return ctx.waitUntil(p); },
        passThroughOnException() {
          if (ctx && typeof ctx.passThroughOnException === "function") ctx.passThroughOnException();
        },
      };
      let rp;
      fake.respondWith = (r) => { rp = r; };
      for (const h of hs) h(fake);
      return rp;
    };
  }
  return fb;
}
function __wrapDefault(d) {
  if (d === undefined || d === null) return __fallback();
  // class-style default export (WorkerEntrypoint): ctx arrives via
  // the constructor — subclass wraps it.
  if (typeof d === "function") {
    return class extends d {
      constructor(ctx, env, ...rest) { super(__wrapCtx(ctx), env, ...rest); }
    };
  }
  if (typeof d === "object") {
    return new Proxy(d, {
      get(t, k) {
        const v = t[k];
        return typeof v === "function" ? __wrapHandler(v, t) : v;
      },
    });
  }
  __wrapped = false;
  return d;
}
export default __wrapDefault(__user.default);
"#,
    );
    out.push_str(&format!("export * from {};\n", js_str(&spec)));
    out.push_str(&format!(
        "export class {PROBE_ENTRYPOINT} extends WorkerEntrypoint {{\n  fetch() {{\n    return Response.json({{ pending: __pending(), wrapped: __wrapped && !__wrapFailed() }});\n  }}\n}}\n"
    ));
    for (i, class) in classes.iter().enumerate() {
        let key = js_str(class);
        out.push_str(&format!(
            "const __WU{i} = typeof __user[{key}] === \"function\"\n  ? class extends __user[{key}] {{\n      constructor(ctx, env, ...rest) {{ super(__wrapCtx(ctx), env, ...rest); }}\n    }}\n  : __user[{key}];\nexport {{ __WU{i} as {class} }};\n"
        ));
    }
    out
}

/// The `vrt-wu-probe` service worker — aggregates one bounded fan-out
/// across every wrapped worker's `VrtWaituntilProbe` entrypoint. Any
/// worker whose entrypoint is unreachable or reports `wrapped:false`
/// counts as `failed` (the supervisor treats that as probe_failed and
/// stops waiting — fail closed, never a hang).
pub fn probe_js(binding_names: &[String]) -> String {
    let mut targets = String::from("[");
    for (i, b) in binding_names.iter().enumerate() {
        if i > 0 {
            targets.push_str(", ");
        }
        targets.push_str(&js_str(b));
    }
    targets.push(']');
    format!(
        "// vrt platform waitUntil aggregate probe (MIG-12). No tenant\n// bindings, deny-all globalOutbound — platform-only by construction.\nconst __TARGETS = {targets};\nexport default {{\n  async fetch(req, env) {{\n    const out = {{ pending: 0, failed: 0 }};\n    await Promise.all(__TARGETS.map(async (k) => {{\n      try {{\n        const r = await env[k].fetch(\"http://vrt-wu{PROBE_PATH}\", {{\n          signal: AbortSignal.timeout(1500),\n        }});\n        const j = await r.json();\n        if (!r.ok || typeof j.pending !== \"number\" || j.wrapped === false) {{\n          out.failed += 1;\n          return;\n        }}\n        out.pending += j.pending;\n      }} catch {{\n        out.failed += 1;\n      }}\n    }}));\n    return Response.json(out);\n  }},\n}};\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_js_exports_probe_and_shadows_class_names() {
        let js = entry_js(
            "index.js",
            &["Counter".to_string(), "OrderFlow".to_string()],
        );
        assert!(js.contains("export class VrtWaituntilProbe extends WorkerEntrypoint"));
        assert!(js.contains("export * from \"./index.js\""));
        assert!(js.contains("export { __WU0 as Counter };"));
        assert!(js.contains("export { __WU1 as OrderFlow };"));
        assert!(js.contains("typeof __user[\"Counter\"] === \"function\""));
    }

    #[test]
    fn entry_js_no_classes() {
        let js = entry_js("src/main.mjs", &[]);
        assert!(js.contains("import * as __user from \"./src/main.mjs\";"));
        assert!(!js.contains("__WU0"));
    }

    #[test]
    fn probe_js_lists_every_binding() {
        let js = probe_js(&["VRT_WU_0".into(), "VRT_WU_2".into()]);
        assert!(js.contains("\"VRT_WU_0\", \"VRT_WU_2\""));
        assert!(js.contains(PROBE_PATH));
        assert!(js.contains("j.wrapped === false"));
    }

    #[test]
    fn prelude_is_syntax_valid_and_tracks() {
        // Not a JS parser — a structural check that the counting
        // contract pieces exist and the exported names line up with
        // what entry_js imports.
        for name in [
            "__track",
            "__pending",
            "__wrapCtx",
            "__wrapFailed",
            "__listenersOf",
        ] {
            assert!(PRELUDE_JS.contains(&format!("export function {name}")));
        }
        assert!(PRELUDE_JS.contains("globalThis.addEventListener"));
    }

    #[test]
    fn wrap_ctx_is_in_place_not_proxy() {
        // workerd base classes reject a Proxy ctx — the real object
        // must be passed down with `waitUntil` replaced in place.
        assert!(!PRELUDE_JS.contains("new Proxy(ctx"));
        assert!(PRELUDE_JS.contains("Object.defineProperty(ctx, \"waitUntil\""));
        assert!(PRELUDE_JS.contains("const __WRAPPED = Symbol("));
        assert!(PRELUDE_JS.contains("ctx.waitUntil[__WRAPPED] === true"));
        assert!(PRELUDE_JS.contains("__failed = true"));
        let js = entry_js("index.js", &["Counter".to_string()]);
        assert!(!js.contains("new Proxy(ctx"));
        assert!(js.contains("wrapped: __wrapped && !__wrapFailed()"));
        assert!(js.contains("super(__wrapCtx(ctx), env, ...rest)"));
    }
}
