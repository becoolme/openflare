// facade-router: Miniflare-order routing, bounded index cache, fail-closed
// 503, marker handling. Exercises dist bytes like every other facade test.
import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule } from "./helpers.mjs";

const { AssetsRouter } = await import(facadeModule("facade-router.js"));

const HDR = "x-vrt-asset-route";

function userWorker(calls, body = "worker") {
  return {
    async fetch(req) {
      calls.push(req.url);
      return new Response(body, { status: 200 });
    },
  };
}

function assetsWorker(calls, index, body = "asset") {
  // fetchRouteIndex echoes __digest — wired by env() to the cfg
  // manifestDigest (or overridden by a test for the E3 mismatch case).
  return {
    __digest: undefined,
    async fetch(req) {
      calls.push(req.url);
      return new Response(body, { status: 200 });
    },
    async fetchRouteIndex() {
      return { ...index, manifestDigest: this.__digest };
    },
    async canFetchAsset() {
      return true;
    },
  };
}

let seq = 0;
function env(cfg, user, assets) {
  // Module-level index cache is keyed by manifestDigest — give each
  // test a fresh 64-hex digest so cache state never leaks between
  // tests (compileCfg rejects non-sha256 digests as config corruption).
  const digest =
    cfg.manifestDigest ??
    (seq++).toString(16).padStart(64, "0");
  // Provider echoes the manifest digest in every route-index response
  // (E3); wrap the stub so all fixtures get it unless they deliberately
  // return their own (the mismatch test does).
  if (assets && typeof assets.fetchRouteIndex === "function") {
    const orig = assets.fetchRouteIndex.bind(assets);
    assets.fetchRouteIndex = async (input) => {
      const idx = await orig(input);
      if (
        idx && typeof idx === "object" && !("manifestDigest" in idx)
      ) {
        return { ...idx, manifestDigest: assets.__digest ?? digest };
      }
      return idx;
    };
  }
  if (assets && assets.__digest === undefined) assets.__digest = digest;
  return {
    VRT_ROUTER_CFG: JSON.stringify({ ...cfg, manifestDigest: digest }),
    USER_WORKER: user,
    ASSETS: assets,
  };
}

function req(path, headers = {}) {
  return new Request(`http://w.test${path}`, { headers });
}

// WorkerEntrypoint shape: env arrives via the constructor, fetch takes
// only the request — mirror how workerd instantiates the class.
function hit(envObj, request) {
  return new AssetsRouter({}, envObj).fetch(request);
}

const INDEX = {
  htmlHandling: "auto-trailing-slash",
  notFoundHandling: "none",
  paths: ["/app.js", "/about.html", "/dir/index.html"],
};

test("asset hit serves from assets and carries the marker", async () => {
  const u = [], a = [];
  const r = new AssetsRouter();
  const res = await hit(env({}, userWorker(u), assetsWorker(a, INDEX)),req("/app.js"));
  assert.equal(res.status, 200);
  assert.equal(await res.text(), "asset");
  assert.equal(res.headers.get(HDR), "assets");
  assert.deepEqual(a, ["http://w.test/app.js"]);
  assert.equal(u.length, 0);
});

test("miss falls through to the user worker", async () => {
  const u = [], a = [];
  const r = new AssetsRouter();
  const res = await hit(env({}, userWorker(u), assetsWorker(a, INDEX)),req("/nope"));
  assert.equal(await res.text(), "worker");
  assert.equal(u.length, 1);
  assert.equal(a.length, 0);
});

test("html_handling: /about matches /about.html", async () => {
  const a = [];
  const r = new AssetsRouter();
  const res = await hit(env({}, userWorker([]), assetsWorker(a, INDEX)),req("/about"));
  assert.equal(res.headers.get(HDR), "assets");
});

test("html_handling: /dir matches /dir/index.html (307 fetchable)", async () => {
  const a = [];
  const r = new AssetsRouter();
  const res = await hit(env({}, userWorker([]), assetsWorker(a, INDEX)),req("/dir"));
  assert.equal(res.headers.get(HDR), "assets");
});

test("force-trailing-slash: every HTML form still routes to assets", async () => {
  const idx = {
    htmlHandling: "force-trailing-slash",
    notFoundHandling: "none",
    paths: ["/about.html", "/dir/index.html"],
  };
  for (const p of ["/about", "/about.html", "/about/", "/dir", "/dir/", "/dir/index", "/dir/index.html"]) {
    const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req(p));
    assert.equal(res.headers.get(HDR), "assets", p);
  }
});

test("drop-trailing-slash: every HTML form still routes to assets", async () => {
  const idx = {
    htmlHandling: "drop-trailing-slash",
    notFoundHandling: "none",
    paths: ["/about.html", "/dir/index.html"],
  };
  for (const p of ["/about", "/about.html", "/about/", "/dir", "/dir/", "/dir/index", "/dir/index.html"]) {
    const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req(p));
    assert.equal(res.headers.get(HDR), "assets", p);
  }
});

test("dotted HTML stems route to their .html alias", async () => {
  // `/release-1.2` → `/release-1.2.html`: a dot does not make the
  // path a concrete asset, and the redirect destination must stay
  // fetchable (matches vrt-provider resolve()).
  const idx = { ...INDEX, paths: ["/release-1.2.html", "/app.js"] };
  for (const p of ["/release-1.2", "/release-1.2.html"]) {
    const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req(p));
    assert.equal(res.headers.get(HDR), "assets", p);
  }
});

test("overlapping page and directory routes both route to assets", async () => {
  const idx = { ...INDEX, paths: ["/both.html", "/both/index.html"] };
  for (const p of ["/both", "/both/", "/both.html", "/both/index", "/both/index.html"]) {
    const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req(p));
    assert.equal(res.headers.get(HDR), "assets", p);
  }
});

test("shadowed canonical forms fall through to the user worker", async () => {
  // `/file-bin` exists, so the `/file-bin.html` rewrite has no free
  // destination: the provider 404s `/file-bin/`, `/file-bin/index` and
  // `/file-bin/index.html` in auto/drop mode. The router must not claim
  // them for assets (worker fallback instead of a 404 from assets).
  for (const htmlHandling of ["auto-trailing-slash", "drop-trailing-slash"]) {
    const idx = { htmlHandling, notFoundHandling: "none", paths: ["/file-bin", "/file-bin.html"] };
    for (const p of ["/file-bin", "/file-bin.html"]) {
      const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req(p));
      assert.equal(res.headers.get(HDR), "assets", `${htmlHandling} ${p}`);
    }
    for (const p of ["/file-bin/", "/file-bin/index", "/file-bin/index.html"]) {
      const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req(p));
      assert.equal(res.headers.get(HDR), null, `${htmlHandling} ${p}`);
    }
  }
  // force mode serves/redirects `/file-bin.html` at `/file-bin/`, so all
  // forms stay on the assets side.
  const force = {
    htmlHandling: "force-trailing-slash",
    notFoundHandling: "none",
    paths: ["/file-bin", "/file-bin.html"],
  };
  for (const p of ["/file-bin", "/file-bin.html", "/file-bin/", "/file-bin/index", "/file-bin/index.html"]) {
    const res = await hit(env({}, userWorker([]), assetsWorker([], force)), req(p));
    assert.equal(res.headers.get(HDR), "assets", p);
  }
});

test("_redirects matching uses the raw request path", async () => {
  // The provider matches `_redirects` against the un-decoded path, so the
  // router must agree: an encoded path that only matches after decoding
  // belongs to the worker, and an encoded segment that encodes a `/`
  // still matches a `:name` rule.
  const idx = {
    ...INDEX,
    paths: ["/app.js"],
    redirects: [
      { source: "/old", status: 302 },
      { source: "/en/:slug", status: 302 },
    ],
  };
  const encodedLiteral = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/%6Fld"));
  assert.equal(encodedLiteral.headers.get(HDR), null);
  const plain = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/old"));
  assert.equal(plain.headers.get(HDR), "assets");
  // `/en/a%2Fb` is one raw segment → matches `/en/:slug`.
  const encodedSlash = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/en/a%2Fb"));
  assert.equal(encodedSlash.headers.get(HDR), "assets");
  // The decoded form has two segments and does not match.
  const decodedSlash = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/en/a/b"));
  assert.equal(decodedSlash.headers.get(HDR), null);
});

test("exact extensionless /index file routes to assets", async () => {
  const idx = { ...INDEX, paths: ["/api/index"] };
  const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/api/index"));
  assert.equal(res.headers.get(HDR), "assets");
});

test("html_handling none: only exact asset paths route to assets", async () => {
  const idx = {
    htmlHandling: "none",
    notFoundHandling: "none",
    paths: ["/about.html", "/app.js"],
  };
  const miss = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/about"));
  assert.equal(miss.headers.get(HDR), null);
  const hitExact = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/about.html"));
  assert.equal(hitExact.headers.get(HDR), "assets");
});

test("_redirects sources route to assets before the worker", async () => {
  const idx = {
    htmlHandling: "auto-trailing-slash",
    notFoundHandling: "none",
    paths: ["/new.html"],
    redirects: [
      { source: "/old", status: 301 },
      { source: "/blog/*", status: 302 },
      { source: "/m/:id", status: 307 },
    ],
  };
  for (const p of ["/old", "/blog/a/b", "/m/42"]) {
    const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req(p));
    assert.equal(res.headers.get(HDR), "assets", p);
  }
  // Non-matching paths still fall through.
  const miss = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/nope"));
  assert.equal(miss.headers.get(HDR), null);
});

test("_routes.json include runs the worker even when an asset exists", async () => {
  const idx = {
    htmlHandling: "auto-trailing-slash",
    notFoundHandling: "none",
    paths: ["/index.html", "/api/users", "/api/public/data.txt"],
    routes: { include: ["/api/*"], exclude: ["/api/public/*"] },
  };
  // include wins over asset existence.
  const api = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/api/users"));
  assert.equal(api.headers.get(HDR), null);
  // exclude wins over include.
  const excluded = await hit(
    env({}, userWorker([]), assetsWorker([], idx)),
    req("/api/public/data.txt"),
  );
  assert.equal(excluded.headers.get(HDR), "assets");
  // Unmatched paths default to assets, with no existence probe.
  for (const p of ["/index.html", "/missing"]) {
    const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req(p));
    assert.equal(res.headers.get(HDR), "assets", p);
  }
});

test("_routes.json matches the raw request path", async () => {
  const idx = {
    htmlHandling: "none",
    notFoundHandling: "none",
    paths: [],
    routes: { include: ["/api/*"], exclude: [] },
  };
  const encoded = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/%61pi/x"));
  assert.equal(encoded.headers.get(HDR), "assets"); // no decode before match
  const plain = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/api/x"));
  assert.equal(plain.headers.get(HDR), null);
});

test("explicit run_worker_first rules take precedence over _routes.json", async () => {
  const idx = {
    htmlHandling: "auto-trailing-slash",
    notFoundHandling: "none",
    paths: ["/index.html", "/api/public/data.txt"],
    routes: { include: ["/api/*"], exclude: ["/api/public/*"] },
  };
  // The user-worker rule matches even though `_routes.json` excludes it.
  const cfg = { userWorker: ["/api/*"] };
  const excluded = await hit(
    env(cfg, userWorker([]), assetsWorker([], idx)),
    req("/api/public/data.txt"),
  );
  assert.equal(excluded.headers.get(HDR), null);
  // Unmatched paths no longer use the `_routes.json` default: assets
  // still wins on existence, and a miss falls through to the worker.
  const asset = await hit(env(cfg, userWorker([]), assetsWorker([], idx)), req("/index.html"));
  assert.equal(asset.headers.get(HDR), "assets");
  const miss = await hit(env(cfg, userWorker([]), assetsWorker([], idx)), req("/missing"));
  assert.equal(miss.headers.get(HDR), null);
});

test("malformed _routes.json fails closed", async () => {
  for (const routes of [
    { include: [] },
    { include: ["/api/*/x"], exclude: [] },
    { include: ["/api/*"], exclude: ["x"] },
    { include: ["/" + "a".repeat(101)] },
  ]) {
    const idx = { ...INDEX, routes };
    const res = await hit(env({}, userWorker([]), assetsWorker([], idx)), req("/app.js"));
    assert.equal(res.status, 503, JSON.stringify(routes));
    assert.equal(JSON.parse(await res.text()).error.code, "ASSETS_ROUTE_UNAVAILABLE");
  }
});

test("SPA fallback only applies to navigate requests", async () => {
  const idx = { ...INDEX, notFoundHandling: "single-page-application", paths: ["/index.html"] };
  const nav = await hit(env({}, userWorker([]), assetsWorker([], idx)),req("/deep/route", { "sec-fetch-mode": "navigate" }));
  assert.equal(nav.headers.get(HDR), "assets");
  const plain = await hit(env({}, userWorker([]), assetsWorker([], idx)),req("/deep/route"));
  assert.equal(plain.headers.get(HDR), null);
});

test("404-page serves the nearest ancestor 404.html", async () => {
  const idx = { ...INDEX, notFoundHandling: "404-page", paths: ["/docs/404.html"] };
  const res = await hit(env({}, userWorker([]), assetsWorker([], idx)),req("/docs/missing", { "sec-fetch-mode": "navigate" }));
  assert.equal(res.headers.get(HDR), "assets");
  const miss = await hit(env({}, userWorker([]), assetsWorker([], idx)),req("/other/missing", { "sec-fetch-mode": "navigate" }));
  assert.equal(miss.headers.get(HDR), null);
});

test("static routing: asset_worker `!` rules win over user_worker", async () => {
  const u = [], a = [];
  const cfg = { userWorker: ["/*"], assetWorker: ["!/api/*"] };
  const r = new AssetsRouter();
  const first = await hit(env(cfg, userWorker(u), assetsWorker(a, INDEX)),req("/anything"));
  assert.equal(await first.text(), "worker");
  const api = await hit(env(cfg, userWorker(u), assetsWorker(a, INDEX)),req("/api/x"));
  assert.equal(await api.text(), "worker");
  assert.equal(u.length, 2);
});

test("static routing: user_worker rules bypass the probe", async () => {
  const u = [], a = [];
  const cfg = { assetWorker: ["!*"], userWorker: ["/*"] };
  // "*" rule shape: asset rules are the !-negated forms; user /* wins.
  const res = await hit(env(cfg, userWorker(u), assetsWorker(a, INDEX)),req("/app.js"));
  assert.equal(await res.text(), "worker");
  assert.equal(a.length, 0); // probe never consulted
});

test("invokeUserWorkerAhead goes to the worker without probing", async () => {
  const u = [], a = [];
  const cfg = { invokeUserWorkerAhead: true };
  const res = await hit(env(cfg, userWorker(u), assetsWorker(a, INDEX)),req("/app.js"));
  assert.equal(await res.text(), "worker");
  assert.equal(a.length, 0);
});

test("non-GET methods never hit assets", async () => {
  const u = [], a = [];
  const res = await hit(env({}, userWorker(u), assetsWorker(a, INDEX)),new Request("http://w.test/app.js", { method: "POST" }));
  assert.equal(await res.text(), "worker");
  assert.equal(a.length, 0);
});

test("index fetch failure fails closed: 503 + typed code + marker, worker untouched", async () => {
  const u = [];
  const broken = {
    async fetch() { throw new Error("must not be called"); },
    async fetchRouteIndex() { throw new Error("provider down"); },
    async canFetchAsset() { throw new Error("provider down"); },
  };
  const res = await hit(env({}, userWorker(u), broken),req("/app.js"));
  assert.equal(res.status, 503);
  const body = JSON.parse(await res.text());
  assert.equal(body.error.code, "ASSETS_ROUTE_UNAVAILABLE");
  assert.equal(res.headers.get(HDR), "assets"); // metered as route, not worker
  assert.equal(u.length, 0); // fail-closed: no silent worker fallback
});

test("malformed index fails closed the same way", async () => {
  const u = [];
  const bad = {
    async fetch() { throw new Error("unreachable"); },
    async fetchRouteIndex() { return { htmlHandling: 42 }; },
    async canFetchAsset() { return false; },
  };
  const res = await hit(env({}, userWorker(u), bad),req("/app.js"));
  assert.equal(res.status, 503);
  assert.equal(u.length, 0);
});

test("oversized index degrades to the single canFetchAsset probe", async () => {
  const a = [];
  let probeCalls = 0;
  const big = {
    async fetch(req) { a.push(req.url); return new Response("asset"); },
    async fetchRouteIndex() {
      // > MAX_INDEX_BYTES via approxBytes accumulation
      return { htmlHandling: "none", notFoundHandling: "none",
               paths: ["/" + "x".repeat(2040)] };
    },
    async canFetchAsset() { probeCalls++; return true; },
  };
  // paths.length ≤ 2048 each; build an index whose approxBytes exceed 4MB
  // by pushing many paths — but paths cap is 4096×2048 ≈ 8MB > 4MB.
  const huge = { htmlHandling: "none", notFoundHandling: "none",
                 paths: Array.from({ length: 3000 }, (_, i) => `/${i}-${"y".repeat(2040)}`) };
  big.fetchRouteIndex = async () => huge;
  const res = await hit(env({}, userWorker([]), big),req("/1-zzz"));
  assert.equal(probeCalls, 1);
  assert.equal(res.headers.get(HDR), "assets");
});

test("user worker responses cannot leak the route marker", async () => {
  const forged = {
    async fetch() {
      return new Response("worker", { headers: { [HDR]: "assets" } });
    },
  };
  const res = await hit(env({ invokeUserWorkerAhead: true }, forged, assetsWorker([], INDEX)),req("/app.js"));
  assert.equal(res.headers.get(HDR), null);
  assert.equal(await res.text(), "worker");
});

test("index is cached across requests (one fetchRouteIndex call)", async () => {
  let indexCalls = 0;
  const a = [];
  const counting = {
    async fetch(req) { a.push(req.url); return new Response("asset"); },
    async fetchRouteIndex() { indexCalls++; return INDEX; },
    async canFetchAsset() { return false; },
  };
  const r = new AssetsRouter();
  const e = env({}, userWorker([]), counting); // same digest → cache reuse
  await hit(e,req("/app.js"));
  await hit(e,req("/about"));
  await hit(e,req("/dir"));
  assert.equal(indexCalls, 1);
  assert.equal(a.length, 3);
});

// ── T31d: tooLarge degrade, cfg corruption, memoized compile, decode ──

const INDEX_4097 = {
  htmlHandling: "none",
  notFoundHandling: "none",
  paths: Array.from({ length: 4097 }, (_, i) => `/f${i}.txt`),
};

test("4097-path index degrades to canFetchAsset — hit → assets", async () => {
  let probes = 0;
  const u = [], a = [];
  const big = {
    async fetch(req) { a.push(req.url); return new Response("asset"); },
    async fetchRouteIndex() { return INDEX_4097; },
    async canFetchAsset() { probes++; return true; },
  };
  const res = await hit(env({}, userWorker(u), big),req("/f1.txt"));
  assert.equal(probes, 1); // measured degradation, not 503
  assert.equal(await res.text(), "asset");
  assert.equal(u.length, 0);
});

test("4097-path index degrades — miss → worker", async () => {
  const u = [];
  const big = {
    async fetch() { throw new Error("unreachable"); },
    async fetchRouteIndex() { return INDEX_4097; },
    async canFetchAsset() { return false; },
  };
  const res = await hit(env({}, userWorker(u), big),req("/nope"));
  assert.equal(await res.text(), "worker");
});

test("provider tooLarge flag degrades the same way", async () => {
  let probes = 0;
  const a = [];
  const flag = {
    async fetch(req) { a.push(req.url); return new Response("asset"); },
    async fetchRouteIndex() {
      return { htmlHandling: "none", notFoundHandling: "none", tooLarge: true };
    },
    async canFetchAsset() { probes++; return true; },
  };
  const res = await hit(env({}, userWorker([]), flag),req("/x"));
  assert.equal(probes, 1);
  assert.equal(res.headers.get(HDR), "assets");
});

test("missing manifestDigest fails closed (config corruption)", async () => {
  const u = [];
  const e = {
    VRT_ROUTER_CFG: JSON.stringify({ invokeUserWorkerAhead: false }), // no digest
    USER_WORKER: userWorker(u),
    ASSETS: assetsWorker([], INDEX),
  };
  const res = await hit(e,req("/app.js"));
  assert.equal(res.status, 503);
  assert.equal(JSON.parse(await res.text()).error.code, "ASSETS_ROUTE_UNAVAILABLE");
  assert.equal(u.length, 0);
});

test("non-hex manifestDigest fails closed the same way", async () => {
  const u = [];
  const e = {
    VRT_ROUTER_CFG: JSON.stringify({ manifestDigest: "not-hex" }),
    USER_WORKER: userWorker(u),
    ASSETS: assetsWorker([], INDEX),
  };
  const res = await hit(e,req("/app.js"));
  assert.equal(res.status, 503);
  assert.equal(u.length, 0);
});

test("malformed VRT_ROUTER_CFG JSON → typed 503 not 500", async () => {
  const u = [];
  const e = {
    VRT_ROUTER_CFG: "{not json",
    USER_WORKER: userWorker(u),
    ASSETS: assetsWorker([], INDEX),
  };
  const res = await hit(e,req("/app.js"));
  assert.equal(res.status, 503);
  assert.equal(JSON.parse(await res.text()).error.code, "ASSETS_ROUTE_UNAVAILABLE");
  assert.equal(u.length, 0);
});

test("same cfg string compiles once across requests", async () => {
  const { __cfgCompileCountForTest } = await import(
    facadeModule("router-core.js")
  );
  const before = __cfgCompileCountForTest();
  const e = env(
    { userWorker: ["/api/*"], assetWorker: ["!/api/static/*"] },
    userWorker([]),
    assetsWorker([], INDEX),
  );
  const r = new AssetsRouter();
  await hit(e,req("/a"));
  await hit(e,req("/b"));
  await hit(e,req("/c"));
  assert.equal(__cfgCompileCountForTest() - before, 1); // JSON.parse + regexes once
});

test("percent-encoded paths decode before manifest match", async () => {
  const a = [];
  const idx = { ...INDEX, paths: ["/my file.txt", "/\u4e2d\u6587 page.html"] };
  const r = new AssetsRouter();
  const space = await hit(env({}, userWorker([]), assetsWorker(a, idx)),req("/my%20file.txt"));
  assert.equal(space.headers.get(HDR), "assets");
  const cn = await hit(env({}, userWorker([]), assetsWorker(a, idx)),req("/%E4%B8%AD%E6%96%87%20page.html"));
  assert.equal(cn.headers.get(HDR), "assets");
});

test("malformed percent escape keeps raw segment (no crash, no match)", async () => {
  const r = new AssetsRouter();
  const res = await hit(env({}, userWorker([]), assetsWorker([], INDEX)),req("/bad%zzfile"));
  assert.equal(res.headers.get(HDR), null); // miss → worker
  assert.equal(await res.text(), "worker");
});

// ── T31e: E1 shared decode table, E2 tooLarge cache, E3 digest pin, E4 enums ──

// E1: identical case list to vrt-provider assets.rs
// percent_decode_before_normalize — decodeURIComponent parity.
const DECODE_TABLE = [
  ["/my%20file.txt", "/my file.txt"],
  ["/%E4%B8%AD%E6%96%87%20page.html", "/\u4e2d\u6587 page.html"],
  ["/bad%zzfile", "/bad%zzfile"], // bad escape → whole segment raw
  ["/a%zz%20b", "/a%zz%20b"], // mixed: one bad escape → NOTHING decodes
  ["/truncated%2", "/truncated%2"], // `%` + <2 hex → raw
  ["/x%", "/x%"], // lone `%` → raw
  ["/%E4%B8", "/%E4%B8"], // truncated UTF-8 → raw
  ["/a%2Fb.txt", "/a/b.txt"], // decoded '/' is a real separator
];

test("decode table matches provider segment-for-segment", async () => {
  const core = await import(facadeModule("router-core.js"));
  for (const [enc, dec] of DECODE_TABLE) {
    assert.equal(
      core.normalizePath(enc),
      dec,
      `${enc} must decode to ${dec} (identical on the provider side)`,
    );
  }
  // %2E%2E → decoded `..` → traversal rejected; %00 → NUL rejected;
  // %5C → backslash rejected — fail closed identically on both sides.
  for (const bad of ["/%2E%2E/etc", "/%00", "/%5C"]) {
    assert.equal(core.normalizePath(bad), null, `${bad} must fail closed`);
  }
});

test("tooLarge marker cached — repeat requests skip the index RPC", async () => {
  let indexCalls = 0, probes = 0;
  const assets = {
    async fetch() { return new Response("asset"); },
    async fetchRouteIndex() {
      indexCalls++;
      return {
        htmlHandling: "none",
        notFoundHandling: "none",
        tooLarge: true,
      };
    },
    async canFetchAsset() { probes++; return true; },
  };
  const e = env({}, userWorker([]), assets);
  await hit(e,req("/a"));
  await hit(e,req("/b"));
  await hit(e,req("/c"));
  assert.equal(indexCalls, 1); // E2: one index fetch per digest
  assert.equal(probes, 3);     // every request still probed
});

test("index digest mismatch fails closed and never caches", async () => {
  let indexCalls = 0;
  const assets = {
    async fetch() { return new Response("asset"); },
    async fetchRouteIndex() {
      indexCalls++;
      return { ...INDEX, manifestDigest: "f".repeat(64) };
    },
    async canFetchAsset() { return false; },
  };
  const e = env({}, userWorker([]), assets); // cfg digest ≠ f*64
  const r1 = await hit(e,req("/app.js"));
  assert.equal(r1.status, 503);
  const r2 = await hit(e,req("/app.js"));
  assert.equal(r2.status, 503);
  assert.equal(indexCalls, 2); // nothing cached across the mismatch
});

test("index missing manifestDigest fails closed", async () => {
  const assets = {
    async fetch() { return new Response("asset"); },
    async fetchRouteIndex() {
      return { ...INDEX, manifestDigest: undefined }; // key present, value absent
    },
    async canFetchAsset() { return false; },
  };
  const e = env({}, userWorker([]), assets);
  const res = await hit(e,req("/app.js"));
  assert.equal(res.status, 503);
});

test("unknown htmlHandling / notFoundHandling fails closed", async () => {
  for (const key of ["htmlHandling", "notFoundHandling"]) {
    const assets = {
      async fetch() { return new Response("asset"); },
      async fetchRouteIndex() { return { ...INDEX, [key]: "weird-mode" }; },
      async canFetchAsset() { return false; },
    };
    const res = await hit(env({}, userWorker([]), assets),req("/app.js"));
    assert.equal(res.status, 503, `${key} enum drift must fail closed`);
  }
});

// ── T31f/F2: cache accounting + in-flight dedup ──

test("concurrent cold-cache requests share ONE index fetch", async () => {
  let indexCalls = 0;
  const assets = {
    async fetch() { return new Response("asset"); },
    async fetchRouteIndex() {
      indexCalls++;
      await new Promise((r) => setTimeout(r, 5)); // widen the race window
      return INDEX;
    },
    async canFetchAsset() { return false; },
  };
  const e = env({}, userWorker([]), assets);
  const results = await Promise.all(
    Array.from({ length: 10 }, (_, i) => hit(e, req(`/app.js?i=${i}`))),
  );
  assert.equal(indexCalls, 1); // dedup'd, not 10
  for (const res of results) assert.equal(await res.text(), "asset");
});

test("same-key overwrite keeps bytes accounting exact", async () => {
  const core = await import(facadeModule("router-core.js"));
  const digest = "9".repeat(64);
  const entry = {
    kind: "index",
    index: {
      htmlHandling: "none",
      notFoundHandling: "none",
      paths: new Set(["/a.js"]),
      approxBytes: 1000,
      digest,
      manifestDigest: digest,
    },
  };
  core.cachePut(entry);
  const before = core.__cacheInvariantForTest();
  for (let i = 0; i < 100; i++) core.cachePut(entry);
  const inv = core.__cacheInvariantForTest();
  assert.equal(inv.consistent, true, "bytes ledger must equal sum(entries)");
  assert.equal(
    inv.bytes - before.bytes,
    0,
    "100 overwrites add zero net bytes (old entry deducted each time)",
  );
});

test("oversized single index never evicts healthy entries", async () => {
  const core = await import(facadeModule("router-core.js"));
  const healthy = "b".repeat(64);
  core.cachePut({
    kind: "index",
    index: {
      htmlHandling: "none",
      notFoundHandling: "none",
      paths: new Set(["/h.js"]),
      approxBytes: 500,
      digest: healthy,
      manifestDigest: healthy,
    },
  });
  const before = core.__cacheInvariantForTest();
  // A single index over the 4MB bound must be skipped, not inserted-
  // then-evict-everything (which cleared other tenants' entries).
  core.cachePut({
    kind: "index",
    index: {
      htmlHandling: "none",
      notFoundHandling: "none",
      paths: new Set(["/big.js"]),
      approxBytes: 5 * 1024 * 1024,
      digest: "c".repeat(64),
      manifestDigest: "c".repeat(64),
    },
  });
  const after = core.__cacheInvariantForTest();
  assert.equal(after.consistent, true);
  assert.equal(after.entries, before.entries, "healthy entries untouched");
  assert.equal(after.bytes, before.bytes);
});

test("failed index fetch frees the in-flight slot — next request retries", async () => {
  let indexCalls = 0;
  const assets = {
    async fetch() { return new Response("asset"); },
    async fetchRouteIndex() {
      indexCalls++;
      if (indexCalls === 1) throw new Error("backend down");
      return INDEX;
    },
    async canFetchAsset() { return false; },
  };
  const e = env({}, userWorker([]), assets);
  const first = await hit(e, req("/app.js"));
  assert.equal(first.status, 503); // failure logged + fail closed
  const second = await hit(e, req("/app.js"));
  assert.equal(await second.text(), "asset"); // retried, now cached
  assert.equal(indexCalls, 2);
});

// ── T31g/G1.3: bounded RPC awaits — a hung backend must 503, never stall ──

test("route-index RPC timeout → 503 route_index_timeout", async () => {
  const core = await import(facadeModule("router-core.js"));
  const digest = (seq++).toString(16).padStart(64, "0");
  const calls = [];
  const hanging = {
    async fetch(req) { return new Response("never", { status: 200 }); },
    async fetchRouteIndex() { return new Promise(() => {}); }, // never settles
    async canFetchAsset() { return true; },
  };
  const e = env(
    { manifestDigest: digest },
    userWorker(calls),
    hanging,
  );
  const prev = core.RPC_BUDGET.indexMs;
  core.RPC_BUDGET.indexMs = 15;
  try {
    const res = await hit(e, req("http://w/app.js"));
    assert.equal(res.status, 503);
    assert.equal(
      res.headers.get("x-vrt-route-error"),
      "route_index_timeout",
    );
  } finally {
    core.RPC_BUDGET.indexMs = prev;
  }
});

test("can-fetch RPC timeout → 503 can_fetch_timeout", async () => {
  const core = await import(facadeModule("router-core.js"));
  const digest = (seq++).toString(16).padStart(64, "0");
  const calls = [];
  const e = env(
    { manifestDigest: digest },
    userWorker(calls),
    {
      async fetch(req) { return new Response("never", { status: 200 }); },
      // tooLarge index forces the can-fetch degradation path.
      async fetchRouteIndex() {
        return {
          htmlHandling: "none",
          notFoundHandling: "none",
          manifestDigest: digest,
          tooLarge: true,
        };
      },
      async canFetchAsset() { return new Promise(() => {}); },
    },
  );
  const prev = core.RPC_BUDGET.probeMs;
  core.RPC_BUDGET.probeMs = 15;
  try {
    const res = await hit(e, req("http://w/app.js"));
    assert.equal(res.status, 503);
    assert.equal(res.headers.get("x-vrt-route-error"), "can_fetch_timeout");
  } finally {
    core.RPC_BUDGET.probeMs = prev;
  }
});

// ── T31h/H1: a settled race releases its armed timer ──

test("withTimeout cancels the armed timer when the RPC wins", async () => {
  const core = await import(facadeModule("router-core.js"));
  const before = core.__timersPendingForTest.n;
  const out = await core.withTimeout(Promise.resolve("ok"), 60_000, "t");
  assert.equal(out, "ok");
  // The abort propagates as a microtask — give the scheduler one turn.
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(
    core.__timersPendingForTest.n,
    before,
    "armed timer survived a settled race",
  );
});

test("withTimeout cancels the armed timer when the RPC rejects", async () => {
  const core = await import(facadeModule("router-core.js"));
  const before = core.__timersPendingForTest.n;
  await assert.rejects(
    core.withTimeout(Promise.reject(new Error("boom")), 60_000, "t"),
    /boom/,
  );
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(
    core.__timersPendingForTest.n,
    before,
    "armed timer survived a rejected race",
  );
});

test("withTimeout still fires on time and releases the timer", async () => {
  const core = await import(facadeModule("router-core.js"));
  const before = core.__timersPendingForTest.n;
  const t0 = Date.now();
  await assert.rejects(
    core.withTimeout(new Promise(() => {}), 20, "idx"),
    (e) => e instanceof core.RpcTimeoutError,
  );
  assert.ok(Date.now() - t0 < 1000, "timeout did not fire near its budget");
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(core.__timersPendingForTest.n, before);
});

test("sleep honours AbortSignal on both implementations", async () => {
  const core = await import(facadeModule("router-core.js"));
  const before = core.__timersPendingForTest.n;
  const ac = new AbortController();
  const p = core.sleep(60_000, ac.signal);
  const t0 = Date.now();
  ac.abort();
  await assert.rejects(p, /abort/i);
  assert.ok(Date.now() - t0 < 1000, "abort did not settle sleep early");
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(core.__timersPendingForTest.n, before);
});
