// T31f/F0 contract test: the REAL `AssetTransport` (facade-assets.js)
// wired to the REAL `AssetsRouter` (facade-router.js). The ONLY stub
// is BINDING_BACKEND, which answers in the provider's real wire
// format (vrt-provider route-index / can-fetch / typed error codes).
// Each side's own tests stub the other end, so the RPC contract
// itself was never executed end-to-end until this file.
import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule, makeBackend, makeProps } from "./helpers.mjs";

const { AssetTransport } = await import(facadeModule("facade-assets.js"));
const { AssetsRouter } = await import(facadeModule("facade-router.js"));

const ROUTE_INDEX_URL = "http://binding-backend/internal/assets/v1/route-index";
const CAN_FETCH_URL = "http://binding-backend/internal/assets/v1/can-fetch";
const FETCH_URL = "http://binding-backend/internal/assets/v1/fetch";

let seq = 0;
// Module-level index cache is keyed by manifestDigest — each rig gets
// a fresh 64-hex digest so cache state never leaks between tests.
function digest() {
  return (seq++).toString(16).padStart(64, "0");
}

// BINDING_BACKEND stub speaking the provider's real wire format:
//   POST fetch        → asset body
//   POST route-index  → {htmlHandling, notFoundHandling, manifestDigest, paths}
//   POST can-fetch    → literal "true"/"false"
//   errors            → !ok + `x-open-compute-error-code` header
function rig(handlers = {}) {
  const DIGEST = handlers.digest ?? digest();
  const INDEX = {
    htmlHandling: "none",
    notFoundHandling: "none",
    manifestDigest: DIGEST,
    paths: ["/app.js", "/about.html"],
  };
  const calls = [];
  const backend = {
    async fetch(url, init = {}) {
      const headers = {};
      new Headers(init.headers).forEach((v, k) => (headers[k] = v));
      calls.push({ url, method: init.method ?? "GET", headers });
      if (url === ROUTE_INDEX_URL) {
        if (handlers.routeIndexError) {
          return new Response("err", {
            status: 503,
            headers: { "x-open-compute-error-code": handlers.routeIndexError },
          });
        }
        const routeIndex =
          typeof handlers.routeIndex === "function"
            ? handlers.routeIndex(DIGEST)
            : (handlers.routeIndex ?? INDEX);
        return new Response(JSON.stringify(routeIndex), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      if (url === CAN_FETCH_URL) {
        if (handlers.canFetchError) {
          return new Response("err", {
            status: 503,
            headers: { "x-open-compute-error-code": handlers.canFetchError },
          });
        }
        return new Response(String(handlers.canFetch ?? true), { status: 200 });
      }
      if (url === FETCH_URL) {
        return new Response(handlers.assetBody ?? "asset-body", { status: 200 });
      }
      throw new Error(`unexpected backend url ${url}`);
    },
  };
  // The REAL assets transport — only its BINDING_BACKEND is stubbed.
  const assets = new AssetTransport(
    { props: makeProps({ namespaceResourceId: DIGEST }) },
    { BINDING_BACKEND: backend },
  );
  const userCalls = [];
  const env = {
    VRT_ROUTER_CFG: JSON.stringify({ manifestDigest: DIGEST }),
    USER_WORKER: {
      async fetch(req) {
        userCalls.push(req.url);
        return new Response("user-worker");
      },
    },
    // The RPC surface the router sees — the real transport instance,
    // exactly like a named-entrypoint service binding in workerd.
    ASSETS: assets,
  };
  return { calls, userCalls, env };
}

function router(env) {
  return new AssetsRouter({}, env);
}
function req(path, headers = {}) {
  return new Request(`http://w.test${path}`, { headers });
}

test("contract: index hit serves the asset THROUGH the real transport", async () => {
  const { calls, userCalls, env } = rig();
  const res = await router(env).fetch(req("/app.js"));
  assert.equal(res.status, 200);
  assert.equal(await res.text(), "asset-body");
  assert.equal(res.headers.get("x-vrt-asset-route"), "assets");
  // Backend saw exactly the route-index + fetch calls the provider
  // would — the real transport encoded them.
  assert.equal(calls[0].url, ROUTE_INDEX_URL);
  assert.equal(calls[1].url, FETCH_URL);
  assert.equal(userCalls.length, 0);
});

test("contract: miss falls through to the user worker", async () => {
  const { calls, userCalls, env } = rig();
  const res = await router(env).fetch(req("/missing.js"));
  assert.equal(await res.text(), "user-worker");
  assert.equal(calls.length, 1); // index only — no fetch, no probe
  assert.equal(userCalls.length, 1);
});

test("contract: tooLarge degrades to can-fetch through the real transport", async () => {
  const { calls, env } = rig({
    routeIndex: (DIGEST) => ({
      htmlHandling: "none",
      notFoundHandling: "none",
      manifestDigest: DIGEST,
      tooLarge: true,
    }),
    canFetch: true,
  });
  const res = await router(env).fetch(req("/anything"));
  assert.equal(await res.text(), "asset-body");
  assert.deepEqual(
    calls.map((c) => c.url),
    [ROUTE_INDEX_URL, CAN_FETCH_URL, FETCH_URL],
  );
});

test("contract: backend typed error → 503, never a silent worker fallback", async () => {
  const { userCalls, env } = rig({ routeIndexError: "ASSET_STORAGE_UNAVAILABLE" });
  const res = await router(env).fetch(req("/app.js"));
  assert.equal(res.status, 503);
  const body = JSON.parse(await res.text());
  assert.equal(body.error.code, "ASSETS_ROUTE_UNAVAILABLE");
  assert.equal(
    res.headers.get("x-vrt-route-error"),
    "route_index_rpc_failed",
  );
  assert.equal(userCalls.length, 0);
});

test("contract: digest-mismatched index → 503 with bounded reason", async () => {
  const { env } = rig({
    routeIndex: {
      htmlHandling: "none",
      notFoundHandling: "none",
      manifestDigest: "f".repeat(64),
      paths: ["/app.js"],
    },
  });
  const res = await router(env).fetch(req("/app.js"));
  assert.equal(res.status, 503);
  assert.equal(res.headers.get("x-vrt-route-error"), "route_index_digest_mismatch");
});

test("contract: invalid index body → 503 with bounded reason", async () => {
  const { env } = rig({ routeIndex: { htmlHandling: 42 } });
  const res = await router(env).fetch(req("/app.js"));
  assert.equal(res.status, 503);
  assert.equal(res.headers.get("x-vrt-route-error"), "route_index_invalid");
});

test("contract: can-fetch RPC failure → 503 with bounded reason", async () => {
  const { env } = rig({
    routeIndex: (DIGEST) => ({
      htmlHandling: "none",
      notFoundHandling: "none",
      manifestDigest: DIGEST,
      tooLarge: true,
    }),
    canFetchError: "ASSET_STORAGE_UNAVAILABLE",
  });
  const res = await router(env).fetch(req("/app.js"));
  assert.equal(res.status, 503);
  assert.equal(res.headers.get("x-vrt-route-error"), "can_fetch_rpc_failed");
});
