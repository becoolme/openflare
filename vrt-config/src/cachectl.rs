//! `ctx.cache` façade (Workers Cache control) — per-worker shim.
//!
//! workerd's `ExecutionContext` has no native `cache` property, so the
//! platform attachs it in the existing waitUntil wrapper (modules[0]) when
//! the version enables caching. The wrapper's `ctx.cache.purge(opts)` calls
//! this per-worker shim over a service binding; the shim injects the
//! platform-resolved `{tenantId, workerId}` scope (never caller input) and
//! forwards to the host's private control-listener purge endpoint with the
//! shared `VRT_CACHE_PURGE_TOKEN`.
//!
//! Trust boundary: the shim runs platform code and is the only holder of the
//! token; tenant code can call the binding but cannot read the token or
//! change the injected scope. The host re-validates exactly one purge mode
//! (`workerId` default, or tenant-scoped `tags`/`pathPrefix`/`all`) with the
//! same `wc_admin` bounds. Without a control listener the shim answers 503 —
//! never a fake success.

/// Tenant-facing binding name attached by the wrapper.
pub const SHIM_BINDING: &str = "VRT_CACHE_CTL";
/// capnp service name prefix of the per-worker shim.
pub const SHIM_SERVICE_PREFIX: &str = "vrt-cachectl";
/// Shared external service name the shim calls (host control listener).
pub const DEL_SERVICE: &str = "vrt-cachedel";
/// Same env name vrt-host reads for `/internal/cache/purge`.
pub const TOKEN_ENV: &str = "VRT_CACHE_PURGE_TOKEN";

/// capnp service name of one worker's cache-control shim.
pub fn shim_service_name(worker_id: &str) -> String {
    format!("{SHIM_SERVICE_PREFIX}-{worker_id}")
}

/// Inline shim module. Strict body (one mode, same bounds as `wc_admin`);
/// scope is injected as `VRT_CACHE_SCOPE`; the token as a `fromEnvironment`
/// binding; the host address via `VRT_CACHEDEL`.
pub const SHIM_JS: &str = r#"// vrt platform ctx.cache control shim — platform-owned, deny-all outbound.
function bad(code, status) {
  return new Response(JSON.stringify({ error: { code } }), {
    status,
    headers: { "content-type": "application/json" },
  });
}
export default {
  async fetch(req, env) {
    if (req.method !== "POST") return bad("cachectl.method", 405);
    if (!env.VRT_CACHEDEL) return bad("cachectl.unconfigured", 503);
    const scope =
      typeof env.VRT_CACHE_SCOPE === "string"
        ? JSON.parse(env.VRT_CACHE_SCOPE)
        : env.VRT_CACHE_SCOPE;
    if (!scope || typeof scope.tenantId !== "string" || typeof scope.workerId !== "string") {
      return bad("cachectl.scope", 500);
    }
    let opts;
    try {
      const text = await req.text();
      opts = text ? JSON.parse(text) : {};
    } catch {
      return bad("cachectl.json", 400);
    }
    if (opts === null || typeof opts !== "object" || Array.isArray(opts)) {
      return bad("cachectl.body", 400);
    }
    for (const k of Object.keys(opts)) {
      if (k !== "tags" && k !== "pathPrefix" && k !== "all") {
        return bad("cachectl.unknown", 400);
      }
    }
    const modes = [
      opts.tags !== undefined,
      opts.pathPrefix !== undefined,
      opts.all === true,
    ].filter(Boolean).length;
    if (modes > 1) return bad("cachectl.mode", 400);
    let body;
    if (opts.tags !== undefined) {
      if (
        !Array.isArray(opts.tags) ||
        opts.tags.length === 0 ||
        opts.tags.length > 1000 ||
        opts.tags.some((t) => typeof t !== "string" || t.length === 0 || t.length > 1024)
      ) {
        return bad("cachectl.tags", 400);
      }
      body = { tenantId: scope.tenantId, tags: opts.tags };
    } else if (opts.pathPrefix !== undefined) {
      if (
        typeof opts.pathPrefix !== "string" ||
        !opts.pathPrefix.startsWith("/") ||
        opts.pathPrefix.length > 2048
      ) {
        return bad("cachectl.prefix", 400);
      }
      body = { tenantId: scope.tenantId, pathPrefix: opts.pathPrefix };
    } else if (opts.all === true) {
      body = { tenantId: scope.tenantId, all: true };
    } else {
      // Default: purge this worker (all versions/entrypoints).
      body = { tenantId: scope.tenantId, workerId: scope.workerId };
    }
    const res = await env.VRT_CACHEDEL.fetch("http://cachedel/internal/cache/purge", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-vrt-cache-token": env.VRT_CACHE_TOKEN || "",
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      // Do not leak host detail; the status class is enough for the caller.
      return bad("cachectl.upstream", res.status === 403 ? 502 : res.status);
    }
    return res;
  },
};
"#;
