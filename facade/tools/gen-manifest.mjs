#!/usr/bin/env node
// Generate facade/dist/ + facade/manifest.json (schemaVersion 3).
//
// Pipeline (deterministic):
//   1. walk the static relative-import graph of each system worker's TypeScript
//      entry module (vendored open-compute uses `.js` specifiers for `.ts`
//      files, so logical module names are repo-relative paths with `.ts`→`.js`)
//   2. transpile every module with Node's built-in
//      `module.stripTypeScriptTypes(src, { mode: "transform" })` — the bytes
//      workerd embeds. Real workerd does NOT strip TypeScript; publishing
//      source bytes would fail to load (`kv/native-adapter.ts` even uses a
//      parameter property). Emitted files keep the module's relative path
//      under facade/dist/ (the dist root mirrors the repo root), so `.js`
//      specifiers resolve unchanged and need no rewriting.
//   3. re-scan the emitted dist graph and prove the published closure is
//      identical to the source closure (same modules, same externals).
//   4. record per module: logical `name`, `dist`/`source` repo-relative paths,
//      `sha256` over the DIST bytes (what workerd embeds), `sourceSha256`
//      over the TypeScript source (provenance only).
//
// PREBUILT workers (`prebuilt: {dir, main}`) skip steps 1-3: their modules are
// vendored upstream build artifacts already in place under `dist/` (the
// kitesurf engine — `import(` appears inside vendored string literals, so the
// import scanner cannot audit them, and upstream's own pipeline already
// closed the graph). Their module list is a sorted directory enumeration
// hashed verbatim; trust and drift control come from the vendor pin
// (third_party/kitesurf/UPSTREAM.md) + manifest sha256s.
//
// Rules enforced (fail closed):
//   - every import specifier is relative AND resolves to a file inside the
//     repo (workerd embeds modules by name; nothing may escape the graph)
//   - bare specifiers are only workerd-provided externals
//     (`cloudflare:*`, `cloudflare-internal:*`, `node:*`) — recorded, never
//     embedded
//   - no dynamic import()
//   - `import type`/`export type` statements are erased, not resolved
//   - the declared entrypoint must be exported by the entry module
//     (generated workers only — prebuilt modules are opaque)
//
// Usage: node gen-manifest.mjs [--check]
//   default : clean-rebuild dist/ and rewrite manifest.json (vendored
//             subtrees in VENDORED_DIST_DIRS are preserved, not cleaned)
//   --check : rebuild everything in memory and byte-compare against the
//             committed dist/ + manifest.json; exit 1 on any drift
//             (missing/extra/changed file), exit 0 when identical.
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { stripTypeScriptTypes } from "node:module";

const HERE = dirname(fileURLToPath(import.meta.url));
const FACADE_DIR = resolve(HERE, "..");
const ROOT = resolve(HERE, "../..");
const DIST_DIR = resolve(FACADE_DIR, "dist");
const OUT_FILE = resolve(FACADE_DIR, "manifest.json");
const NODE_MAJOR = process.versions.node.split(".")[0];

// Per-invocation budgets for system workers — rendered as
// `Worker.limits.{cpuMs,memoryMb}` by vrt-config (0/absent would mean
// unlimited on that dimension). Only JS-turn CPU counts; I/O wait is
// free. Memory accounts V8 heap + external bytes (ArrayBuffer and WASM
// backing stores), so decode bitmaps and buffered payloads are counted.
// CPU tiers:
//   IMAGES_CPU_MS  — the heavy path: jSquash decode/encode + resvg font
//                    rasterization on tenant-controlled bytes; a legit
//                    64MP transform can burn seconds, so the budget must
//                    bound pathology without false-killing big requests.
//   EGRESS_CPU_MS  — buffered /transform multiparts (≤64MiB) are scanned
//                    byte-wise in JS for text markers, plus payload fetches.
//   DEFAULT_CPU_MS — thin RPC facades; bounds glue pathology only.
// Memory tiers (legit envelopes derived from the pixel/byte caps in
// facade/images-wire.ts + facade/egress/cf-image.ts):
//   IMAGES_MEMORY_MB — src bytes ≤64MiB + decoded bitmap ≤70MP=280MB +
//                    transform canvas ≤16MP=64MB + wasm module memories +
//                    a few draw overlays (each ≤70MP decoded, ≤16 entries).
//                    ~600-700MB peak; 768 bounds the 16×70MP pathology.
//   EGRESS_MEMORY_MB — ≤64MiB buffered multipart + ≤64MiB fetched
//                    payload + ≤20MiB font + JSON parse, ~1.5x headroom.
//   DEFAULT_MEMORY_MB — tenant default parity; transports stream bodies
//                    (KV values ≤25MiB) so 128 is generous.
// Subrequest tiers — outbound fetches per invocation (service-binding
// calls count; internally-provided services are exempt):
//   HEAVY_SUBREQUESTS — images: src + ≤16 draw overlays + ≤16 fonts;
//                    egress: payload + ≤16 draw fetches + fonts. ~34
//                    legit worst case; 64 bounds an unbounded loop.
//   DEFAULT_SUBREQUESTS — thin facades make ≤2 binding-backend calls
//                    per invocation; 8 is headroom, not a work plan.
const IMAGES_CPU_MS = 10_000;
const EGRESS_CPU_MS = 2_000;
const DEFAULT_CPU_MS = 1_000;
const IMAGES_MEMORY_MB = 768;
const EGRESS_MEMORY_MB = 192;
const DEFAULT_MEMORY_MB = 128;
const HEAVY_SUBREQUESTS = 64;
const DEFAULT_SUBREQUESTS = 8;
// Kitesurf (vendored browser engine — third_party/kitesurf/UPSTREAM.md):
//   CPU — upstream asks 300000ms (packages/engine/wrangler.jsonc); page
//         parse/layout/paint/boa-eval all burn in-engine.
//   MEM — observed ~430MB workerd RSS for a screenshot session (process
//         total; the isolate slice is smaller). 1024MB bounds a page
//         pathology that would otherwise be unbounded.
//   SUB — upstream wants 1000000; clamped to the platform cap 10000
//         (subresource fan-out of a real page is the legit workload).
const KITESURF_CPU_MS = 300_000;
const KITESURF_MEMORY_MB = 1024;
const KITESURF_SUBREQUESTS = 10_000;

// Vendored subtrees under facade/dist/ — build INPUTS that live at their
// published location so the host's `<dist>/<name>` seeding covers them.
// Clean-rebuild preserves them; --check verifies them byte-for-byte.
const VENDORED_DIST_DIRS = new Set(["kitesurf"]);

// Module types for prebuilt workers — the same filename-suffix map
// upstream's scripts/workerd-config.mjs applies to the kitesurf :dist.
// `.js` files not matching a suffix are esModule; anything else fails.
const PREBUILT_MODULE_TYPES = [
  [".wasm", "wasm"],
  [".glue.js", "text"],
  [".css", "text"],
  [".dat", "text"],
  [".woff2", "data"],
  [".bin", "data"],
];

const WORKERS = [
  { serviceName: "facade-kv", entrypoint: "KVNamespace", entry: "facade-kv.ts", needsBindingBackend: true },
  { serviceName: "facade-r2", entrypoint: "R2Transport", entry: "facade-r2.ts", needsBindingBackend: true },
  { serviceName: "facade-assets", entrypoint: "AssetTransport", entry: "facade-assets.ts", needsBindingBackend: true },
  { serviceName: "facade-pg", entrypoint: "PgTransport", entry: "facade-pg.ts", needsBindingBackend: true },
  { serviceName: "facade-d1", entrypoint: "D1Facade", entry: "facade-d1.ts", needsBindingBackend: true },
  { serviceName: "facade-queue", entrypoint: "QueueFacade", entry: "facade-queue.ts", needsBindingBackend: true },
  // K2 durable event log — inner `fetcher` of the k2 wrapped binding
  // (produce-only: POST /send → provider → vrt-sql K2Backend).
  { serviceName: "facade-k2", entrypoint: "K2Facade", entry: "facade-k2.ts", needsBindingBackend: true },
  { serviceName: "facade-email", entrypoint: "EmailFacade", entry: "facade-email.ts", needsBindingBackend: true },
  // CF Images binding — inner `fetcher` of `cloudflare-internal:images-api`.
  // Pure in-worker transforms (vendored WASM engine); no binding-backend.
  { serviceName: "facade-images", entrypoint: "ImagesFacade", entry: "facade-images.ts", needsBindingBackend: false, cpuMs: IMAGES_CPU_MS, memoryMb: IMAGES_MEMORY_MB, subRequests: HEAVY_SUBREQUESTS },
  // CF Vectorize binding — inner `fetcher` of `cloudflare-internal:vectorize-api`
  // (v2 GA wire); forwards ops to the provider's sqlite-vec backend.
  { serviceName: "facade-vectorize", entrypoint: "VectorizeFacade", entry: "facade-vectorize.ts", needsBindingBackend: true },
  // MIG-16: in-process Rate Limiting binding — pure RPC entrypoint, no
  // binding-backend (state is local to the workerd process).
  { serviceName: "facade-ratelimit", entrypoint: "RateLimiter", entry: "facade-ratelimit.ts", needsBindingBackend: false },
  // T37b: consumer delivery bridge — pure RPC proxy (CONSUMER
  // service binding → tenant worker); no binding-backend.
  { serviceName: "facade-qconsumer", entrypoint: "QueueConsumerFacade", entry: "facade-qconsumer.ts", needsBindingBackend: false },
  { serviceName: "facade-router", entrypoint: "AssetsRouter", entry: "facade-router.ts", needsBindingBackend: false },
  // T6: tenant egress gate. No binding-backend — it talks only to the
  // outside world through `outbound-network`, never to platform APIs.
  { serviceName: "vrt-egress", entrypoint: "EgressProxy", entry: "egress/egress.ts", needsBindingBackend: false, cpuMs: EGRESS_CPU_MS, memoryMb: EGRESS_MEMORY_MB, subRequests: HEAVY_SUBREQUESTS },
  // Deny-all globalOutbound target for system workers that must never
  // reach a network at all (facades only talk to BINDING_BACKEND).
  { serviceName: "vrt-no-network", entrypoint: "DenyAll", entry: "egress/deny.ts", needsBindingBackend: false },
  // ── kitesurf: vendored browser engine (third_party/kitesurf/UPSTREAM.md).
  // The engine worker is a PREBUILT upstream `:dist` — page HTML/CSS/JS,
  // layout and paint all run inside it; per-page JS runs in LOADER-spawned
  // anonymous isolates. `entrypoint: ""` = default export (it has no named
  // WorkerEntrypoint). `global_fetch_strictly_public` + `publicOnly` are a
  // pair: the engine fetches page subresources, and only over the public
  // internet (its own PNA guard also defaults closed).
  {
    serviceName: "facade-kitesurf",
    entrypoint: "",
    prebuilt: { dir: "facade/dist/kitesurf/engine", main: "index.js" },
    needsBindingBackend: false,
    compatibilityDate: "2026-09-11",
    compatibilityFlags: ["global_fetch_strictly_public"],
    outbound: "publicOnly",
    cpuMs: KITESURF_CPU_MS,
    memoryMb: KITESURF_MEMORY_MB,
    subRequests: KITESURF_SUBREQUESTS,
    bindings: [
      { type: "workerLoader", name: "LOADER", maxWorkers: 32 },
      { type: "service", name: "FONT_ASSETS", service: "facade-kitesurf-front", entrypoint: "Assets" },
      // Upstream `vars` (wrangler.jsonc) — "" toggles are read as unset.
      // `KITESURF_NATIVE_EVAL` stays empty: page eval runs on boa (the
      // native-eval path needs workerd's `experimental` flag per-worker,
      // which is a platform decision the manifest cannot take).
      { type: "text", name: "KITESURF_ENV", value: "production" },
      { type: "text", name: "KITESURF_VERSION", value: "0affe70c" },
      { type: "text", name: "KITESURF_RESOURCE_DRAIN_QUIET_TURNS", value: "" },
      { type: "text", name: "KITESURF_NATIVE_EVAL", value: "" },
      { type: "text", name: "KITESURF_DISABLE_PARTIAL_RASTER", value: "" },
      { type: "text", name: "KITESURF_PAGE_CPU_MS_CAP", value: "" },
      { type: "text", name: "KITESURF_PAGE_WALL_MS_CAP", value: "" },
      { type: "text", name: "KITESURF_LAG_WATCHDOG", value: "" },
      { type: "text", name: "KITESURF_FONT_PROFILE", value: "" },
      { type: "text", name: "KITESURF_INSPECTOR_BASE_URL", value: "" },
      { type: "text", name: "KITESURF_INSPECTOR_FALLBACK_URL", value: "" },
      { type: "text", name: "KITESURF_ALLOW_PRIVATE_NETWORK", value: "" },
      { type: "text", name: "WEB_BOT_AUTH_SIGNATURE_AGENT", value: "" },
      { type: "text", name: "WEB_BOT_AUTH_JWK", value: "" },
    ],
  },
  // The engine's FONT_ASSETS backend + public/ router — vendored
  // `docker/front.js` verbatim. `PUBLIC` targets the `disk` service;
  // `HEADERS` is compiled from kitesurf-public/_headers at build time.
  {
    serviceName: "facade-kitesurf-front",
    entrypoint: "Assets",
    prebuilt: { dir: "facade/dist/kitesurf", main: "front.js" },
    needsBindingBackend: false,
    compatibilityDate: "2026-09-11",
    outbound: "deny",
    bindings: [
      { type: "service", name: "ENGINE", service: "facade-kitesurf" },
      { type: "service", name: "PUBLIC", service: "facade-kitesurf-public" },
    ],
    // Compiled `_headers` rules → the HEADERS `json` binding. Single
    // source of truth: the vendored _headers file inside the payload.
    headersJsonFrom: "facade/kitesurf-public/_headers",
  },
  // `browser` binding endpoint — CF Browser Rendering `/v1/*` protocol
  // (acquire/attach/sessions/history/limits) over the kitesurf engine.
  // ENGINE goes through facade-kitesurf-front's default export, which
  // routes upgrade + /json requests to the engine and static paths to
  // the disk service. `outbound: deny` — every call is a service binding.
  // Session registry lives in the isolate (ws sockets can't cross
  // requests), so its budget stays thin-tier; tenant scoping comes from
  // ctx.props at bind time, not the manifest.
  {
    serviceName: "facade-browser",
    entrypoint: "BrowserBinding",
    entry: "facade-browser.ts",
    needsBindingBackend: false,
    outbound: "deny",
    bindings: [
      { type: "service", name: "ENGINE", service: "facade-kitesurf-front" },
      {
        type: "json",
        name: "BROWSER_LIMITS",
        value: JSON.stringify({
          maxConcurrentSessions: 4,
          maxSessionMs: 900_000,
          maxKeepAliveMs: 600_000,
          pendingTtlMs: 60_000,
          historyCap: 100,
        }),
      },
    ],
  },
];

// `disk` services (workerd DiskDirectory) — vendored static payload trees
// served to system workers. `path` is FACADE-relative: the host joins it
// under the manifest's own dir and canonicalises at render-params time.
const DISK_SERVICES = [
  { serviceName: "facade-kitesurf-public", path: "kitesurf-public" },
];

const EXTERNAL_PREFIXES = ["cloudflare:", "cloudflare-internal:", "node:"];
const MODULE_EXTS = [".ts", ".js", ".mjs"];

function fail(file, message) {
  throw new Error(`gen-manifest: ${file}: ${message}`);
}

// Strip line/block comments while preserving string and template contents,
// so import specifiers are only collected from real code positions.
function stripComments(source, file) {
  const frames = [{ mode: "code", braces: 0 }];
  let out = "";
  let i = 0;
  const top = () => frames[frames.length - 1];
  while (i < source.length) {
    const c = source[i];
    const n = source[i + 1];
    const f = top();
    if (f.mode === "line") {
      if (c === "\n") { out += c; frames.pop(); }
      i++;
      continue;
    }
    if (f.mode === "block") {
      if (c === "*" && n === "/") { frames.pop(); i += 2; continue; }
      if (c === "\n") out += c;
      i++;
      continue;
    }
    if (f.mode === "sq" || f.mode === "dq") {
      out += c;
      if (c === "\\") { out += n ?? ""; i += 2; continue; }
      if ((f.mode === "sq" && c === "'") || (f.mode === "dq" && c === '"')) frames.pop();
      i++;
      continue;
    }
    if (f.mode === "tpl") {
      out += c;
      if (c === "\\") { out += n ?? ""; i += 2; continue; }
      if (c === "`") { frames.pop(); i++; continue; }
      if (c === "$" && n === "{") { out += n; frames.push({ mode: "code", braces: 0 }); i += 2; continue; }
      i++;
      continue;
    }
    // code
    if (c === "/" && n === "/") { frames.push({ mode: "line" }); i += 2; continue; }
    if (c === "/" && n === "*") { frames.push({ mode: "block" }); i += 2; continue; }
    if (c === "'") { frames.push({ mode: "sq" }); out += c; i++; continue; }
    if (c === '"') { frames.push({ mode: "dq" }); out += c; i++; continue; }
    if (c === "`") { frames.push({ mode: "tpl" }); out += c; i++; continue; }
    if (c === "{") { f.braces++; out += c; i++; continue; }
    if (c === "}") {
      out += c;
      if (f.braces === 0 && frames.length > 1) frames.pop();
      else f.braces--;
      i++;
      continue;
    }
    out += c;
    i++;
  }
  if (frames.length !== 1) fail(file, "unterminated comment/string/template");
  return out;
}

const TYPE_IMPORT_RE = /\b(?:import|export)\s+type\s[^'";]*?(?:\bfrom\s*["'][^"']+["']|;?)/g;
const STATIC_SPEC_RE = /(?:import|export)\s[^'";]*?\bfrom\s*["']([^"']+)["']|\bimport\s*["']([^"']+)["']/g;
const DYNAMIC_IMPORT_RE = /\bimport\s*\(/;

function importsOf(file, source) {
  const stripped = stripComments(source, file);
  if (DYNAMIC_IMPORT_RE.test(stripped)) fail(file, "dynamic import() is not allowed");
  const code = stripped.replace(TYPE_IMPORT_RE, "");
  const specs = [];
  for (const match of code.matchAll(STATIC_SPEC_RE)) {
    specs.push(match[1] ?? match[2]);
  }
  return specs;
}

// Resolve an import specifier against a concrete file. `container` is the
// directory that must contain every resolved file (repo root for sources,
// dist root for emitted files); `tries` maps a `.js` specifier to the
// candidate module files for that container.
function resolveSpecifier(spec, fromFile, container, tries) {
  if (!spec.startsWith("./") && !spec.startsWith("../")) {
    if (EXTERNAL_PREFIXES.some((p) => spec.startsWith(p))) {
      return { external: spec };
    }
    fail(fromFile, `non-relative specifier ${JSON.stringify(spec)} is not a declared external`);
  }
  const candidate = resolve(dirname(fromFile), spec);
  const inside = relative(container, candidate);
  if (inside.startsWith("..") || inside === "" || inside.includes(`..${sep}`)) {
    fail(fromFile, `specifier ${JSON.stringify(spec)} escapes the module root`);
  }
  // `.wasm` resolves to the literal file only — workerd imports it as a
  // `WebAssembly.Module` (`wasm = embed` in capnp), never scanned or
  // transpiled.
  if (spec.endsWith(".wasm")) {
    if (!existsSync(candidate)) {
      fail(fromFile, `wasm specifier ${JSON.stringify(spec)} does not resolve inside the module graph`);
    }
    return { file: candidate };
  }
  for (const t of tries(candidate)) {
    if (existsSync(t) && MODULE_EXTS.some((e) => t.endsWith(e))) {
      if (t.endsWith(".d.ts")) {
        fail(fromFile, `specifier ${JSON.stringify(spec)} resolves to a declaration file`);
      }
      return { file: t };
    }
  }
  fail(fromFile, `specifier ${JSON.stringify(spec)} does not resolve inside the module graph`);
}

const SOURCE_TRIES = (candidate) => {
  const tries = [candidate];
  if (candidate.endsWith(".js")) {
    tries.push(candidate.slice(0, -3) + ".ts", candidate.slice(0, -3) + ".mjs");
  }
  return tries;
};
// Dist files are already `.js`; a specifier resolves only to the literal file.
const DIST_TRIES = (candidate) => [candidate];

function moduleName(repoRelative) {
  return repoRelative.endsWith(".ts") ? repoRelative.slice(0, -3) + ".js" : repoRelative;
}

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function assertEntrypoint(source, file, entrypoint) {
  const stripped = stripComments(source, file);
  const direct = new RegExp(`\\bexport\\s+(?:async\\s+)?(?:class|const|function|let|var)\\s+${entrypoint}\\b`);
  const braced = new RegExp(`\\bexport\\s*\\{[^}]*\\b${entrypoint}\\b[^}]*\\}`);
  const def = new RegExp(`\\bexport\\s+default\\s+${entrypoint}\\b`);
  if (!direct.test(stripped) && !braced.test(stripped) && !def.test(stripped)) {
    fail(file, `entrypoint ${entrypoint} is not exported by the entry module`);
  }
}

// Walk the static import graph from `entryFile` inside `container`,
// resolving specifiers via `tries`. Returns {files, externals}.
function walk(entryFile, container, tries) {
  const files = new Set();
  const externals = new Set();
  const queue = [entryFile];
  while (queue.length) {
    const file = queue.shift();
    if (files.has(file)) continue;
    files.add(file);
    // Wasm modules are leaves: bytes are embedded verbatim and carry no
    // import graph of their own.
    if (file.endsWith(".wasm")) continue;
    const source = readFileSync(file, "utf8");
    for (const spec of importsOf(file, source)) {
      const resolved = resolveSpecifier(spec, file, container, tries);
      if (resolved.external) externals.add(resolved.external);
      else queue.push(resolved.file);
    }
  }
  return { files, externals };
}

// Transpile one source file to its published bytes.
function publish(source, file) {
  if (file.endsWith(".ts")) {
    const out = stripTypeScriptTypes(source, { mode: "transform" });
    return out.endsWith("\n") ? out : `${out}\n`;
  }
  return source;
}

// Ported verbatim (algorithm only) from upstream kitesurf
// scripts/workerd-config.mjs `headerRules`: compiles Cloudflare's
// `_headers` format into the rule list docker/front.js's `applyHeaderRules`
// consumes. Supports path patterns with `*` splats, `Name: value` and
// `! Name` lines; anything else fails the build.
function headerRules(source, file) {
  const rules = [];
  const lines = source.split(/\r?\n/);
  for (const [index, raw] of lines.entries()) {
    const die = (why) => fail(`${file}:${index + 1}`, `${why}: ${raw}`);
    const line = raw.trim();
    if (line === "" || line.startsWith("#")) continue;
    if (!/^\s/.test(raw)) {
      if (!/^\/[\w\-./*]*$/.test(line)) die("only path patterns with `*` are translated");
      const pattern = `^${line.replaceAll(".", "\\.").replaceAll("*", ".*")}$`;
      rules.push({ pattern, set: [], unset: [] });
      continue;
    }
    const rule = rules.at(-1) ?? die("a header before any path");
    const unset = /^!\s*([\w-]+)$/.exec(line);
    const set = /^([\w-]+):\s*(.*)$/.exec(line);
    if (unset) rule.unset.push(unset[1].toLowerCase());
    else if (set) rule.set.push([set[1].toLowerCase(), set[2]]);
    else die("not a header");
  }
  return rules;
}

// Modules of a PREBUILT worker: the flat file list of `dir`, hashed
// verbatim. `dir` is DIST-relative (vendored payloads are already at
// their published location — see VENDORED_DIST_DIRS).
function prebuiltModules(worker, emitted) {
  const dir = resolve(DIST_DIR, worker.prebuilt.dir.replace(/^facade\/dist\//, ""));
  if (!existsSync(dir)) fail(worker.prebuilt.dir, "prebuilt module dir missing — run the vendor step first");
  const modules = [];
  const files = readdirSync(dir, { withFileTypes: true });
  for (const entry of files) {
    // Subdirectories belong to another worker's prebuilt dir (engine/
    // sits next to front.js); files the --check drift pass flags.
    if (!entry.isFile()) continue;
    const abs = resolve(dir, entry.name);
    const name = `${worker.prebuilt.dir.replace(/^facade\/dist\//, "")}/${entry.name}`;
    const bytes = readFileSync(abs);
    emitted.set(abs, bytes);
    const type = PREBUILT_MODULE_TYPES.find(([suffix]) => entry.name.endsWith(suffix))?.[1];
    if (!type && !entry.name.endsWith(".js")) {
      fail(abs, `no module type for prebuilt file ${entry.name} — extend PREBUILT_MODULE_TYPES deliberately`);
    }
    const rel = relative(ROOT, abs).split(sep).join("/");
    modules.push({
      name,
      dist: rel,
      source: rel, // vendored artifact — provenance is third_party/*/UPSTREAM.md
      sourceSha256: sha256Hex(bytes),
      sha256: sha256Hex(bytes),
      ...(type ? { type } : {}),
    });
  }
  const mainName = `${worker.prebuilt.dir.replace(/^facade\/dist\//, "")}/${worker.prebuilt.main}`;
  if (!modules.some((m) => m.name === mainName)) {
    fail(worker.prebuilt.dir, `prebuilt main ${worker.prebuilt.main} not among the modules`);
  }
  return { modules, mainName };
}

function build() {
  const externals = new Map();
  const workers = {};
  const emitted = new Map(); // absolute dist path -> bytes
  for (const worker of WORKERS) {
    let modules;
    let mainName;
    if (worker.prebuilt) {
      // Vendored artifacts: enumerate + hash verbatim, no transpile and
      // no import-graph audit (see the PREBUILT note in the header).
      ({ modules, mainName } = prebuiltModules(worker, emitted));
    } else {
      const entryFile = resolve(FACADE_DIR, worker.entry);
      if (!existsSync(entryFile)) fail(entryFile, "entry module missing");
      const { files, externals: workerExternals } = walk(entryFile, ROOT, SOURCE_TRIES);
      for (const spec of workerExternals) {
        externals.set(spec, "workerd-provided external");
      }
      assertEntrypoint(readFileSync(entryFile, "utf8"), entryFile, worker.entrypoint);
      mainName = moduleName(relative(ROOT, entryFile).split(sep).join("/"));
      modules = [];
      for (const file of files) {
        const rel = relative(ROOT, file).split(sep).join("/");
        const name = moduleName(rel);
        const distAbs = resolve(DIST_DIR, name);
        const entry = {
          name,
          dist: relative(ROOT, distAbs).split(sep).join("/"),
          source: rel,
        };
        if (file.endsWith(".wasm")) {
          // Raw bytes — no transpile; `wasm = embed` in the rendered capnp.
          const bytes = readFileSync(file);
          emitted.set(distAbs, bytes);
          entry.sourceSha256 = sha256Hex(bytes);
          entry.sha256 = sha256Hex(bytes);
          entry.type = "wasm";
        } else {
          const source = readFileSync(file, "utf8");
          const published = publish(source, file);
          emitted.set(distAbs, published);
          entry.sourceSha256 = sha256Hex(source);
          entry.sha256 = sha256Hex(published);
        }
        modules.push(entry);
      }
    }
    modules.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    if (new Set(modules.map((m) => m.name)).size !== modules.length) {
      fail(worker.serviceName, "duplicate logical module name");
    }
    // `headersJsonFrom`: compile a vendored `_headers` file into the
    // worker's HEADERS `json` binding (single source of truth = the file).
    const bindings = [...(worker.bindings ?? [])];
    if (worker.headersJsonFrom) {
      const headersPath = resolve(ROOT, worker.headersJsonFrom);
      if (!existsSync(headersPath)) fail(worker.headersJsonFrom, "_headers source missing");
      bindings.push({
        type: "json",
        name: "HEADERS",
        value: JSON.stringify(headerRules(readFileSync(headersPath, "utf8"), worker.headersJsonFrom)),
      });
    }
    workers[worker.serviceName] = {
      entrypoint: worker.entrypoint,
      needsBindingBackend: worker.needsBindingBackend,
      mainModule: mainName,
      cpuMs: worker.cpuMs ?? DEFAULT_CPU_MS,
      memoryMb: worker.memoryMb ?? DEFAULT_MEMORY_MB,
      subRequests: worker.subRequests ?? DEFAULT_SUBREQUESTS,
      // Schema v3 optional fields — emitted only when a WORKERS entry
      // declares them (wasm-engine workers: kitesurf).
      ...(worker.compatibilityDate ? { compatibilityDate: worker.compatibilityDate } : {}),
      ...(worker.compatibilityFlags ? { compatibilityFlags: worker.compatibilityFlags } : {}),
      ...(bindings.length ? { bindings } : {}),
      ...(worker.outbound ? { outbound: worker.outbound } : {}),
      modules,
    };
  }

  // Re-scan the emitted graph: published specifiers must close over exactly
  // the emitted set with no new externals and no `.ts`/`.d.ts` references.
  // (Prebuilt workers are opaque vendored artifacts — skipped.)
  for (const worker of WORKERS) {
    if (worker.prebuilt) continue;
    const entryName = workers[worker.serviceName].mainModule;
    const seen = new Set();
    const queue = [resolve(DIST_DIR, entryName)];
    while (queue.length) {
      const file = queue.shift();
      if (seen.has(file)) continue;
      seen.add(file);
      const published = emitted.get(file);
      if (published === undefined) {
        fail(file, "dist graph references a module outside the emitted set");
      }
      // Wasm leaves hold bytes, not an import graph.
      if (file.endsWith(".wasm")) continue;
      for (const spec of importsOf(file, published)) {
        const candidate = resolve(dirname(file), spec);
        if (spec.startsWith("./") || spec.startsWith("../")) {
          const inside = relative(DIST_DIR, candidate);
          if (inside.startsWith("..") || inside === "" || inside.includes(`..${sep}`)) {
            fail(file, `published specifier ${JSON.stringify(spec)} escapes dist/`);
          }
          if (!emitted.has(candidate)) {
            fail(file, `published specifier ${JSON.stringify(spec)} has no emitted module`);
          }
          queue.push(candidate);
        } else if (!externals.has(spec)) {
          fail(file, `published specifier ${JSON.stringify(spec)} is not a declared external`);
        }
      }
    }
    const expected = new Set(
      workers[worker.serviceName].modules.map((m) => resolve(DIST_DIR, m.name)),
    );
    for (const file of expected) {
      if (!seen.has(file)) fail(file, "emitted module unreachable from the worker entry");
    }
  }

  const manifest =
    JSON.stringify(
      {
        schemaVersion: 3,
        generatedBy: "facade/tools/gen-manifest.mjs",
        nodeVersion: NODE_MAJOR,
        externals: Object.fromEntries(
          [...externals.keys()].sort().map((k) => [k, externals.get(k)]),
        ),
        workers,
        diskServices: DISK_SERVICES,
      },
      null,
      2,
    ) + "\n";
  return { manifest, emitted };
}

// Every file currently inside dist/, as dist-relative posix paths.
function diskDistFiles() {
  const out = [];
  const visit = (dir) => {
    if (!existsSync(dir)) return;
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const abs = resolve(dir, entry.name);
      if (entry.isDirectory()) visit(abs);
      else if (entry.isFile()) out.push(abs);
    }
  };
  visit(DIST_DIR);
  return out;
}

const { manifest, emitted } = build();

if (process.argv.includes("--check")) {
  const drift = [];
  const current = existsSync(OUT_FILE) ? readFileSync(OUT_FILE, "utf8") : "";
  if (current !== manifest) drift.push("manifest.json differs from regenerated output");
  const expected = new Set(emitted.keys());
  for (const [file, bytes] of emitted) {
    const onDisk = existsSync(file) ? readFileSync(file) : null;
    const want = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
    if (onDisk === null || !onDisk.equals(want)) {
      drift.push(`${relative(FACADE_DIR, file)} differs (or is missing)`);
    }
  }
  for (const file of diskDistFiles()) {
    if (!expected.has(file)) {
      drift.push(`${relative(FACADE_DIR, file)} is an extra file not produced by the build`);
    }
  }
  if (drift.length) {
    for (const line of drift) console.error(`gen-manifest --check: ${line}`);
    console.error("gen-manifest --check: drift detected; rerun gen-manifest.mjs");
    process.exit(1);
  }
  console.log("gen-manifest --check: manifest.json and dist/ are up to date");
} else {
  // Clean-rebuild, preserving vendored payload subtrees (prebuilt module
  // inputs — deleting them would lose the vendored artifacts).
  if (existsSync(DIST_DIR)) {
    for (const entry of readdirSync(DIST_DIR, { withFileTypes: true })) {
      if (VENDORED_DIST_DIRS.has(entry.name)) continue;
      rmSync(resolve(DIST_DIR, entry.name), { recursive: true, force: true });
    }
  }
  for (const [file, bytes] of [...emitted.entries()].sort()) {
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, bytes);
  }
  writeFileSync(OUT_FILE, manifest);
  console.log(
    `gen-manifest: wrote ${relative(ROOT, OUT_FILE)} and ${emitted.size} dist modules (node ${NODE_MAJOR})`,
  );
}
