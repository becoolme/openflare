// Pure routing internals for facade-router (T31d/E0): cfg compile +
// memoization, bounded index cache, path normalization, fetchable
// resolution, index parsing. Kept OUT of the entry module — real
// workerd requires entry modules to export a WorkerEntrypoint
// subclass or a {fetch} object; helper/test exports on the entry
// module mis-register it (T25/L12). Tests import this module's dist
// bytes directly.

// ── bounded in-isolate index cache (per manifestDigest) ─────────────
export const MAX_INDEX_ENTRIES = 8;
export const MAX_INDEX_BYTES = 4 * 1024 * 1024;
// Wire pairing with vrt-provider `MAX_ROUTE_INDEX_PATHS`
// (assets.rs): both sides apply the same 4096-path bound — the
// provider short-circuits with `tooLarge` instead of streaming a
// multi-MB path list, and this side treats any index exceeding the
// bound the same way (degrade to `canFetchAsset`, never 503).
export const MAX_INDEX_PATHS = 4096;
/// `_redirects` cap (2,000 static + 100 dynamic).
export const MAX_REDIRECTS = 2100;
/// `_routes.json` cap: 100 include+exclude rules, 100 characters each
/// (Cloudflare Pages limits; mirrored by `vrt-provider/assets_routes.rs`).
export const MAX_ROUTES_RULES = 100;
export const MAX_ROUTE_RULE_CHARS = 100;

export interface RedirectRuleWire {
  source: string;
  status: number;
}

/// Pages-style `_routes.json` invocation policy. `exclude` always wins;
/// a path matching neither list stays on the assets side.
export interface RoutesWire {
  include: string[];
  exclude: string[];
}

export interface RouteIndex {
  htmlHandling: string;
  notFoundHandling: string;
  paths: Set<string>;
  /// `_redirects` sources (bounded by the provider). A match is always
  /// fetchable: 3xx returns the redirect, 200 proxies a same-manifest
  /// path.
  redirects: RedirectRuleWire[];
  /// `_routes.json` policy when the manifest ships one. It decides
  /// invocation for every path, so it short-circuits the existence
  /// probe below.
  routes: RoutesWire | null;
  approxBytes: number;
  digest: string;
  /// E3: manifest digest the provider asserts this index describes —
  /// cross-checked against cfg before caching.
  manifestDigest: string;
}

/// Cacheable entry per digest: a parsed index OR a tooLarge marker
/// (T31d/E2 — the marker stops the per-request index+probe double RPC).
export type CacheEntry =
  | { kind: "index"; index: RouteIndex }
  | { kind: "tooLarge"; digest: string; approxBytes: number };

interface CacheSlot {
  entry: CacheEntry;
  lastUsed: number;
}

const indexCache = new Map<string, CacheSlot>();
let indexCacheBytes = 0;
let clock = 0;

export function cacheGet(digest: string): CacheEntry | undefined {
  const slot = indexCache.get(digest);
  if (!slot) return undefined;
  slot.lastUsed = ++clock;
  return slot.entry;
}

function entryBytes(entry: CacheEntry): number {
  return entry.kind === "index" ? entry.index.approxBytes : entry.approxBytes;
}

export function cachePut(entry: CacheEntry): void {
  const key = entry.kind === "index" ? entry.index.digest : entry.digest;
  const bytes = entryBytes(entry);
  // T31f/F2: a single entry over the whole cache bound is never
  // stored — and must NOT evict healthy entries (the previous shape
  // inserted first, then evicted until it had emptied the cache).
  if (bytes > MAX_INDEX_BYTES) return;
  // Overwrite accounting: subtract the evicted entry's bytes before
  // adding the new one's, or `indexCacheBytes` only ever grows.
  const old = indexCache.get(key);
  if (old) indexCacheBytes -= entryBytes(old.entry);
  indexCacheBytes += bytes;
  indexCache.set(key, { entry, lastUsed: ++clock });
  while (indexCache.size > MAX_INDEX_ENTRIES || indexCacheBytes > MAX_INDEX_BYTES) {
    let oldestKey: string | undefined;
    let oldest = Infinity;
    for (const [k, slot] of indexCache) {
      if (slot.lastUsed < oldest) {
        oldest = slot.lastUsed;
        oldestKey = k;
      }
    }
    if (oldestKey === undefined) break;
    const slot = indexCache.get(oldestKey);
    if (!slot) break;
    indexCacheBytes -= entryBytes(slot.entry);
    indexCache.delete(oldestKey);
  }
}

// T31g/G1.3: every RPC the router awaits is bounded — a hung
// provider must surface as a typed 503, never as an indefinite stall.
// `scheduler.wait` is the stable Workers sleep API (verified on
// workerd 2026-09-16); `setTimeout` is the fallback for Node tests.
// The timer runs in the CALLER's own request context, so a timeout
// never shares I/O across requests — each awaiter races the shared
// promise against its own clock.
export class RpcTimeoutError extends Error {
  constructor(what: string) {
    super(`${what} RPC timed out`);
    this.name = "RpcTimeoutError";
  }
}

/// Mutable so tests can shrink the budget; production values are
/// constants. Per-call-site budgets keep probe timeouts distinct from
/// index timeouts in the reason enum.
export const RPC_BUDGET = { indexMs: 5_000, probeMs: 5_000 };

// T31h/H1: timers armed for a race must be cancelled when the race
// settles — otherwise every routed request leaves an armed 5s timer
// in its IoContext. Both paths honour AbortSignal: `scheduler.wait`
// takes `{ signal }` (rejects AbortError on abort — verified on
// workerd 2026-09-16); the `setTimeout` fallback is cleared and
// rejects with the abort reason.
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  const sched = (
    globalThis as {
      scheduler?: {
        wait?: (
          n: number,
          o?: { signal?: AbortSignal },
        ) => Promise<void>;
      };
    }
  ).scheduler;
  if (sched && typeof sched.wait === "function") {
    __timersPendingForTest.n++;
    return sched.wait(ms, signal ? { signal } : undefined).then(
      () => {
        __timersPendingForTest.n--;
      },
      (e) => {
        __timersPendingForTest.n--;
        throw e;
      },
    );
  }
  __timersPendingForTest.n++;
  return new Promise<void>((resolve, reject) => {
    // Once-only settle: the abort listener outlives a fired timer
    // (withTimeout aborts on every settle) — without this guard the
    // pending counter would double-decrement on the timeout path.
    let settled = false;
    const done = (fn: () => void) => {
      if (settled) return;
      settled = true;
      __timersPendingForTest.n--;
      fn();
    };
    const t = setTimeout(() => done(resolve), ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        done(() => reject(signal.reason ?? new Error("aborted")));
      },
      { once: true },
    );
  });
}

export function withTimeout<T>(
  p: Promise<T>,
  ms: number,
  what: string,
): Promise<T> {
  const ac = new AbortController();
  const timer = sleep(ms, ac.signal).then(() => {
    throw new RpcTimeoutError(what);
  });
  // Abort on settle — the loser branch's timer is released whether
  // the RPC won, timed out, or failed. `timer` already has handlers
  // via race, so the abort's AbortError is never unhandled.
  return Promise.race([p, timer]).finally(() => ac.abort());
}

/// Test-only: armed minus settled/cancelled timers created via
/// `sleep`. A `withTimeout` race must return this to 0 on settle —
/// a leftover armed timer is what H1 guards against.
export const __timersPendingForTest = { n: 0 };

// F2: in-flight dedup for route-index fetches — N concurrent requests
// on the same digest share ONE fetch+parse. The promise is removed
// once settled (failures are never cached: a rejected promise frees
// the slot so the next request retries). Dedup never crosses digests.
const indexInflight = new Map<string, Promise<IndexParse>>();

export function fetchRouteIndexDedup(
  digest: string,
  fetcher: () => Promise<unknown>,
): Promise<IndexParse> {
  const existing = indexInflight.get(digest);
  if (existing) return existing;
  const p = (async (): Promise<IndexParse> => parseIndex(await fetcher()))();
  indexInflight.set(digest, p);
  const cleanup = () => {
    if (indexInflight.get(digest) === p) indexInflight.delete(digest);
  };
  p.then(cleanup, cleanup);
  return p;
}

/// Test-only probe: cache accounting invariant —
/// `indexCacheBytes` must equal the sum of stored entry bytes.
export function __cacheInvariantForTest(): {
  entries: number;
  bytes: number;
  consistent: boolean;
} {
  let sum = 0;
  for (const slot of indexCache.values()) sum += entryBytes(slot.entry);
  return {
    entries: indexCache.size,
    bytes: indexCacheBytes,
    consistent: sum === indexCacheBytes,
  };
}

// ── rule matching (glob-only, path-only — router.worker.js:168–177) ──
export const ESCAPE = /[-/\\^$+?.()|[\]{}]/g;
export function globMatch(rules: RegExp[], path: string): boolean {
  for (const re of rules) {
    if (re.test(path)) return true;
  }
  return false;
}

// Percent-decode one path segment — mirror of Miniflare `decodePath`
// (assets.worker.js:1057–1062) and vrt-provider `decode_segment`:
// malformed escapes or non-UTF-8 decodes keep the raw segment.
export function decodeSegment(seg: string): string {
  try {
    return decodeURIComponent(seg);
  } catch {
    return seg;
  }
}

// Mirror of vrt-provider normalize_asset_path — identical byte rules:
// decode each segment FIRST (manifest stores decoded file paths), then
// structural checks.
export function normalizePath(raw: string): string | null {
  if (!raw || raw.length > 2048 || raw.includes("\\") || raw.includes("\0")) return null;
  let out = raw.startsWith("/") ? "" : "/";
  const trailing = raw.endsWith("/") && raw.length > 1;
  const parts = raw.split("/").filter((p) => p !== "");
  for (let i = 0; i < parts.length; i++) {
    const part = decodeSegment(parts[i]);
    if (part === "." || part === ".." || part.includes("\\") || part.includes("\0")) {
      return null;
    }
    if (i > 0 || !out.endsWith("/")) out += "/";
    out += part;
  }
  if (trailing && !out.endsWith("/")) out += "/";
  if (out === "") out = "/";
  // Cloudflare `decodePath` collapses repeated slashes after decoding.
  while (out.includes("//")) out = out.replaceAll("//", "/");
  return out;
}

// Mirror of vrt-provider `html_intent`: the per-mode candidate order
// plus `safeRedirect` validation, so the router's existence decision
// matches what the provider actually serves. Asset identity is the
// request path (the route index carries no digests): a canonical
// rewrite only counts when it would round-trip to the same file.
// Returns the asset path that would be served, the redirect target, or
// null for a miss; `not_found_handling` is applied by the caller.
type HtmlIntent = { asset: string } | { redirect: string } | null;
type Has = (p: string) => boolean;

function findAsset(has: Has, path: string): string | null {
  return has(path) ? path : null;
}

function safeRedirect(
  has: Has,
  mode: string,
  file: string,
  dest: string,
  skip: boolean,
): string | null {
  if (skip || dest === "" || has(dest)) return null;
  const target = findAsset(has, file);
  if (target === null) return null;
  const intent = htmlIntent(has, mode, dest, true);
  return intent !== null && "asset" in intent && intent.asset === target
    ? dest
    : null;
}

function htmlIntent(has: Has, mode: string, path: string, skip: boolean): HtmlIntent {
  if (mode === "none") {
    const asset = findAsset(has, path);
    return asset === null ? null : { asset };
  }
  if (mode === "force-trailing-slash") return htmlForce(has, path, skip);
  if (mode === "drop-trailing-slash") return htmlDrop(has, path, skip);
  return htmlAuto(has, path, skip);
}

/// `auto-trailing-slash` — mirror of `auto_trailing_slash`.
function htmlAuto(has: Has, path: string, skip: boolean): HtmlIntent {
  const exact = findAsset(has, path);
  if (path.endsWith("/index")) {
    if (exact !== null) return { asset: exact };
    const dir = path.slice(0, -"index".length); // `/foo/` or `/`
    const first = safeRedirect(has, "auto-trailing-slash", `${path}.html`, dir, skip);
    if (first !== null) return { redirect: first };
    const stem = dir.replace(/\/+$/, "");
    const second = safeRedirect(has, "auto-trailing-slash", `${stem}.html`, stem, skip);
    if (second !== null) return { redirect: second };
  } else if (path.endsWith("/index.html")) {
    const dir = path.slice(0, -"index.html".length); // `/foo/` or `/`
    const first = safeRedirect(has, "auto-trailing-slash", path, dir, skip);
    if (first !== null) return { redirect: first };
    const stem = dir.replace(/\/+$/, "");
    const second = safeRedirect(has, "auto-trailing-slash", `${stem}.html`, stem, skip);
    if (second !== null) return { redirect: second };
  } else if (path.endsWith("/")) {
    const index = findAsset(has, `${path}index.html`);
    if (index !== null) return { asset: index };
    const stem = path.replace(/\/+$/, "");
    const target = safeRedirect(has, "auto-trailing-slash", `${stem}.html`, stem, skip);
    if (target !== null) return { redirect: target };
  } else if (path.endsWith(".html")) {
    const stem = path.slice(0, -".html".length);
    const first = safeRedirect(has, "auto-trailing-slash", path, stem, skip);
    if (first !== null) return { redirect: first };
    const second = safeRedirect(
      has,
      "auto-trailing-slash",
      `${stem}/index.html`,
      `${stem}/`,
      skip,
    );
    if (second !== null) return { redirect: second };
  }
  if (exact !== null) return { asset: exact };
  const html = findAsset(has, `${path}.html`);
  if (html !== null) return { asset: html };
  const tail = safeRedirect(
    has,
    "auto-trailing-slash",
    `${path}/index.html`,
    `${path}/`,
    skip,
  );
  return tail === null ? null : { redirect: tail };
}

/// `force-trailing-slash` — mirror of `force_trailing_slash`.
function htmlForce(has: Has, path: string, skip: boolean): HtmlIntent {
  const exact = findAsset(has, path);
  if (path.endsWith("/index")) {
    if (exact !== null) return { asset: exact };
    const dir = path.slice(0, -"index".length); // `/foo/` or `/`
    const first = safeRedirect(has, "force-trailing-slash", `${path}.html`, dir, skip);
    if (first !== null) return { redirect: first };
    const stem = dir.replace(/\/+$/, "");
    const second = safeRedirect(has, "force-trailing-slash", `${stem}.html`, dir, skip);
    if (second !== null) return { redirect: second };
  } else if (path.endsWith("/index.html")) {
    const dir = path.slice(0, -"index.html".length);
    const first = safeRedirect(has, "force-trailing-slash", path, dir, skip);
    if (first !== null) return { redirect: first };
    const stem = dir.replace(/\/+$/, "");
    const second = safeRedirect(has, "force-trailing-slash", `${stem}.html`, dir, skip);
    if (second !== null) return { redirect: second };
  } else if (path.endsWith("/")) {
    const index = findAsset(has, `${path}index.html`);
    if (index !== null) return { asset: index };
    const stem = path.replace(/\/+$/, "");
    const html = findAsset(has, `${stem}.html`);
    if (html !== null) return { asset: html };
  } else if (path.endsWith(".html")) {
    const slashed = `${path.slice(0, -".html".length)}/`;
    const first = safeRedirect(has, "force-trailing-slash", path, slashed, skip);
    if (first !== null) return { redirect: first };
    // Both `/foo.html` and `/foo/index.html` exist: the rewrite would
    // land on the index, so the exact file is served at its own URL.
    if (exact !== null) return { asset: exact };
    const second = safeRedirect(
      has,
      "force-trailing-slash",
      `${slashed}index.html`,
      slashed,
      skip,
    );
    if (second !== null) return { redirect: second };
  }
  if (exact !== null) return { asset: exact };
  const slashed = `${path}/`;
  const first = safeRedirect(has, "force-trailing-slash", `${path}.html`, slashed, skip);
  if (first !== null) return { redirect: first };
  const second = safeRedirect(
    has,
    "force-trailing-slash",
    `${path}/index.html`,
    slashed,
    skip,
  );
  return second === null ? null : { redirect: second };
}

/// `drop-trailing-slash` — mirror of `drop_trailing_slash`.
function htmlDrop(has: Has, path: string, skip: boolean): HtmlIntent {
  const exact = findAsset(has, path);
  if (path.endsWith("/index")) {
    if (exact !== null) return { asset: exact };
    const stem = path.slice(0, -"/index".length); // `/foo` or ``
    if (stem === "") {
      const root = safeRedirect(has, "drop-trailing-slash", "/index.html", "/", skip);
      if (root !== null) return { redirect: root };
    } else {
      const first = safeRedirect(has, "drop-trailing-slash", `${stem}.html`, stem, skip);
      if (first !== null) return { redirect: first };
      const second = safeRedirect(has, "drop-trailing-slash", `${path}.html`, stem, skip);
      if (second !== null) return { redirect: second };
    }
  } else if (path.endsWith("/index.html")) {
    if (path === "/index.html") {
      const root = safeRedirect(has, "drop-trailing-slash", "/index.html", "/", skip);
      if (root !== null) return { redirect: root };
    } else {
      const stem = path.slice(0, -"/index.html".length);
      const first = safeRedirect(has, "drop-trailing-slash", path, stem, skip);
      if (first !== null) return { redirect: first };
      if (exact !== null) return { asset: exact };
      const second = safeRedirect(has, "drop-trailing-slash", `${stem}.html`, stem, skip);
      if (second !== null) return { redirect: second };
    }
  } else if (path.endsWith("/")) {
    if (path === "/") {
      const index = findAsset(has, "/index.html");
      if (index !== null) return { asset: index };
    } else {
      const stem = path.replace(/\/+$/, "");
      const first = safeRedirect(has, "drop-trailing-slash", `${stem}.html`, stem, skip);
      if (first !== null) return { redirect: first };
      const second = safeRedirect(
        has,
        "drop-trailing-slash",
        `${stem}/index.html`,
        stem,
        skip,
      );
      if (second !== null) return { redirect: second };
    }
  } else if (path.endsWith(".html")) {
    const stem = path.slice(0, -".html".length);
    const first = safeRedirect(has, "drop-trailing-slash", path, stem, skip);
    if (first !== null) return { redirect: first };
    const second = safeRedirect(
      has,
      "drop-trailing-slash",
      `${stem}/index.html`,
      stem,
      skip,
    );
    if (second !== null) return { redirect: second };
  }
  if (exact !== null) return { asset: exact };
  const html = findAsset(has, `${path}.html`);
  if (html !== null) return { asset: html };
  const index = findAsset(has, `${path}/index.html`);
  return index === null ? null : { asset: index };
}

/// Mirror of the provider's `_redirects` matcher (existence only).
/// Static sources are exact; dynamic ones have one trailing `*` or
/// `:name` segments (validation happens provider-side).
function redirectMatches(rules: readonly RedirectRuleWire[], path: string): boolean {
  for (const rule of rules) {
    if (rule.source === path) return true;
    if (!rule.source.includes("*") && !rule.source.includes(":")) continue;
    const src = rule.source.split("/");
    const req = path.split("/");
    let matched = true;
    for (let i = 0; i < src.length; i++) {
      if (src[i] === "*") return true;
      if (req[i] === undefined) {
        matched = false;
        break;
      }
      if (src[i].startsWith(":")) {
        if (req[i] === "") {
          matched = false;
          break;
        }
        continue;
      }
      if (src[i] !== req[i]) {
        matched = false;
        break;
      }
    }
    if (matched && req.length === src.length) return true;
  }
  return false;
}

/// Mirror of the provider's `_routes.json` matcher. `*` is a trailing,
/// segment-agnostic wildcard (prefix match); everything else is exact.
/// Provider and router must agree on the raw request path.
function routeMatches(rules: readonly string[], path: string): boolean {
  for (const rule of rules) {
    if (rule.endsWith("*")) {
      if (path.startsWith(rule.slice(0, -1))) return true;
    } else if (rule === path) {
      return true;
    }
  }
  return false;
}

/// `_routes.json` invocation decision: `true` runs the user Worker.
/// `exclude` has priority over `include`; unmatched paths use assets.
export function routesWorkerFirst(routes: RoutesWire, path: string): boolean {
  if (routeMatches(routes.exclude, path)) return false;
  return routeMatches(routes.include, path);
}

export function intentFetchable(
  index: RouteIndex,
  path: string,
  navigate: boolean,
  hasStaticRouting: boolean,
  rawPath: string = path,
): boolean {
  // An explicit `run_worker_first` policy (rules or `true`) is the
  // deployment's routing authority and takes precedence over
  // `_routes.json`, mirroring Cloudflare Workers config precedence.
  if (!hasStaticRouting && index.routes !== null) {
    return !routesWorkerFirst(index.routes, rawPath);
  }
  const has = (p: string) => index.paths.has(p);
  if (has(path)) return true;
  // `_redirects` runs before asset lookup, so a redirect-only path is
  // still handled by the assets service. It matches the RAW request
  // path (provider parity), while everything below uses the decoded,
  // normalized path.
  if (redirectMatches(index.redirects, rawPath)) return true;
  const nfh =
    hasStaticRouting || navigate ? index.notFoundHandling : "none";
  // Existence exactly as the provider resolves it: a request the
  // provider would 404 (e.g. `/file-bin/` when `/file-bin` shadows the
  // `/file-bin.html` rewrite) must fall through to the user worker
  // instead of being claimed by assets.
  if (htmlIntent(has, index.htmlHandling, path, false) !== null) return true;
  if (nfh === "single-page-application") return has("/index.html");
  if (nfh === "404-page") {
    // assets.worker.js:1011–1037 — the nearest ANCESTOR 404.html
    // serves: /a/b/c → /a/b/404.html, /a/404.html, /404.html.
    let dir = path;
    while (dir.length > 0) {
      const idx = dir.lastIndexOf("/");
      dir = idx <= 0 ? "" : dir.slice(0, idx);
      if (has(`${dir === "" ? "" : dir}/404.html`)) return true;
    }
    return false;
  }
  return false;
}

interface RouterCfg {
  invokeUserWorkerAhead?: boolean;
  userWorker?: string[];
  assetWorker?: string[];
  manifestDigest?: string;
}

export interface CompiledCfg {
  invokeAhead: boolean;
  userWorker: RegExp[];
  assetWorker: RegExp[];
  manifestDigest: string;
}

// Compiled cfgs memoized by raw string — the JSON.parse and the
// ≤200 RegExp constructions run once per cfg value, not per request.
// Bounded: workerd keeps module state per isolate; cfg changes
// (redeploy) create a new key, so stale entries self-evict at the cap.
const MAX_CFG_ENTRIES = 16;
const cfgCache = new Map<string, CompiledCfg | undefined>();
let cfgCompiles = 0; // test probe — never reset

function compileRule(rule: string): RegExp | undefined {
  try {
    return new RegExp(
      "^" + rule.split("*").map((s) => s.replace(ESCAPE, "\$&")).join(".*") + "$",
    );
  } catch {
    return undefined; // malformed rule never matches (validated upstream)
  }
}

/// Compile `VRT_ROUTER_CFG` once per value. Returns undefined for ANY
/// malformed cfg — unreadable JSON, bad rule shapes, or a missing/
/// non-sha256 `manifestDigest` (config corruption — the router is only
/// emitted for manifests carrying the digest, so absence is never
/// legitimate). Callers treat undefined as route-unavailable.
export function compileCfg(raw: string): CompiledCfg | undefined {
  if (cfgCache.has(raw)) return cfgCache.get(raw);
  cfgCompiles++;
  const compiled = ((): CompiledCfg | undefined => {
    let cfg: RouterCfg;
    try {
      cfg = JSON.parse(raw);
    } catch {
      return undefined;
    }
    if (typeof cfg !== "object" || cfg === null) return undefined;
    const digest = cfg.manifestDigest;
    if (typeof digest !== "string" || !/^[0-9a-f]{64}$/.test(digest)) {
      return undefined;
    }
    const userWorker = Array.isArray(cfg.userWorker) ? cfg.userWorker : [];
    const assetWorker = Array.isArray(cfg.assetWorker) ? cfg.assetWorker : [];
    if (userWorker.length + assetWorker.length > 200) return undefined;
    if (
      userWorker.some((r) => typeof r !== "string" || r.length > 512) ||
      assetWorker.some((r) => typeof r !== "string" || r.length > 512)
    ) {
      return undefined;
    }
    return {
      invokeAhead: cfg.invokeUserWorkerAhead === true,
      userWorker: userWorker.map(compileRule).filter((r): r is RegExp => !!r),
      assetWorker: assetWorker.map(compileRule).filter((r): r is RegExp => !!r),
      manifestDigest: digest,
    };
  })();
  cfgCache.set(raw, compiled);
  if (cfgCache.size > MAX_CFG_ENTRIES) {
    // FIFO evict — insertion-ordered Map; cfg churn is bounded.
    const oldest = cfgCache.keys().next();
    if (!oldest.done) cfgCache.delete(oldest.value);
  }
  return compiled;
}

// E4: whitelist — mirrors provider html_wire/nfh_wire output.
const HTML_HANDLINGS = new Set([
  "auto-trailing-slash",
  "force-trailing-slash",
  "drop-trailing-slash",
  "none",
]);
const NOT_FOUND_HANDLINGS = new Set(["none", "404-page", "single-page-application"]);
const DIGEST_HEX = /^[0-9a-f]{64}$/;

export type IndexParse =
  | { kind: "index"; index: RouteIndex }
  | { kind: "tooLarge" } // exceeds the cacheable bound → degrade, not 503
  | { kind: "invalid" }; // malformed payload → fail closed 503

function parseRoutesWire(value: unknown): RoutesWire | null | undefined {
  if (value === undefined || value === null) return null;
  if (typeof value !== "object" || Array.isArray(value)) return undefined;
  const { include, exclude } = value as { include?: unknown; exclude?: unknown };
  if (!Array.isArray(include) || include.length === 0) return undefined;
  if (exclude !== undefined && !Array.isArray(exclude)) return undefined;
  const inc = include as unknown[];
  const exc = (exclude ?? []) as unknown[];
  if (inc.length + exc.length > MAX_ROUTES_RULES) return undefined;
  const valid = (rule: unknown): rule is string => {
    if (typeof rule !== "string" || rule.length === 0 || rule.length > MAX_ROUTE_RULE_CHARS) {
      return false;
    }
    if (!rule.startsWith("/") || rule.includes("?") || rule.includes("#")) return false;
    if (!/^[\x20-\x7e]+$/.test(rule)) return false;
    const star = rule.indexOf("*");
    return star === -1 || star === rule.length - 1;
  };
  if (!inc.every(valid) || !exc.every(valid)) return undefined;
  return { include: inc as string[], exclude: exc as string[] };
}

export function parseIndex(raw: unknown): IndexParse {
  if (typeof raw !== "object" || raw === null) return { kind: "invalid" };
  const doc = raw as {
    htmlHandling?: unknown;
    notFoundHandling?: unknown;
    paths?: unknown;
    redirects?: unknown;
    routes?: unknown;
    tooLarge?: unknown;
    manifestDigest?: unknown;
  };
  if (
    typeof doc.htmlHandling !== "string" ||
    !HTML_HANDLINGS.has(doc.htmlHandling) ||
    typeof doc.notFoundHandling !== "string" ||
    !NOT_FOUND_HANDLINGS.has(doc.notFoundHandling)
  ) {
    return { kind: "invalid" };
  }
  // E3: every route-index response must name the manifest it
  // describes; absence is malformed.
  if (typeof doc.manifestDigest !== "string" || !DIGEST_HEX.test(doc.manifestDigest)) {
    return { kind: "invalid" };
  }
  const md = doc.manifestDigest;
  const routes = parseRoutesWire(doc.routes);
  if (routes === undefined) return { kind: "invalid" };
  // Provider-side cap: same bound as MAX_INDEX_PATHS — an oversized
  // manifest gets `tooLarge` without streaming its path list.
  if (doc.tooLarge === true) return { kind: "tooLarge" };
  if (!Array.isArray(doc.paths)) return { kind: "invalid" };
  if (doc.paths.length > MAX_INDEX_PATHS) return { kind: "tooLarge" };
  const paths = new Set<string>();
  let approxBytes = 64;
  for (const p of doc.paths) {
    if (typeof p !== "string" || p.length > 2048) return { kind: "invalid" };
    paths.add(p);
    approxBytes += p.length + 8;
  }
  const redirects: RedirectRuleWire[] = [];
  if (doc.redirects !== undefined) {
    if (!Array.isArray(doc.redirects) || doc.redirects.length > MAX_REDIRECTS) {
      return { kind: "invalid" };
    }
    for (const r of doc.redirects) {
      if (typeof r !== "object" || r === null) return { kind: "invalid" };
      const { source, status } = r as { source?: unknown; status?: unknown };
      if (
        typeof source !== "string" ||
        !source.startsWith("/") ||
        source.length > 1024 ||
        (status !== 200 &&
          status !== 301 &&
          status !== 302 &&
          status !== 303 &&
          status !== 307 &&
          status !== 308)
      ) {
        return { kind: "invalid" };
      }
      redirects.push({ source, status });
      approxBytes += source.length + 16;
    }
  }
  if (routes !== null) {
    approxBytes +=
      routes.include.reduce((n, r) => n + r.length + 4, 8) +
      routes.exclude.reduce((n, r) => n + r.length + 4, 0);
  }
  if (approxBytes > MAX_INDEX_BYTES) return { kind: "tooLarge" };
  return {
    kind: "index",
    index: {
      htmlHandling: doc.htmlHandling,
      notFoundHandling: doc.notFoundHandling,
      paths,
      redirects,
      routes,
      approxBytes,
      digest: "", // set by the caller (cfg.manifestDigest)
      manifestDigest: md,
    },
  };
}

/// Test-only probe: how many cfg strings have been compiled in this
/// isolate (proves memoization without monkey-patching).
export function __cfgCompileCountForTest(): number {
  return cfgCompiles;
}
