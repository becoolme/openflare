export const MAX_INDEX_ENTRIES = 8;
export const MAX_INDEX_BYTES = 4 * 1024 * 1024;
export const MAX_INDEX_PATHS = 4096;
const indexCache = new Map();
let indexCacheBytes = 0;
let clock = 0;
export function cacheGet(digest) {
    const slot = indexCache.get(digest);
    if (!slot) return undefined;
    slot.lastUsed = ++clock;
    return slot.entry;
}
function entryBytes(entry) {
    return entry.kind === "index" ? entry.index.approxBytes : entry.approxBytes;
}
export function cachePut(entry) {
    const key = entry.kind === "index" ? entry.index.digest : entry.digest;
    const bytes = entryBytes(entry);
    if (bytes > MAX_INDEX_BYTES) return;
    const old = indexCache.get(key);
    if (old) indexCacheBytes -= entryBytes(old.entry);
    indexCacheBytes += bytes;
    indexCache.set(key, {
        entry,
        lastUsed: ++clock
    });
    while(indexCache.size > MAX_INDEX_ENTRIES || indexCacheBytes > MAX_INDEX_BYTES){
        let oldestKey;
        let oldest = Infinity;
        for (const [k, slot] of indexCache){
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
export class RpcTimeoutError extends Error {
    constructor(what){
        super(`${what} RPC timed out`);
        this.name = "RpcTimeoutError";
    }
}
export const RPC_BUDGET = {
    indexMs: 5_000,
    probeMs: 5_000
};
export function sleep(ms, signal) {
    const sched = globalThis.scheduler;
    if (sched && typeof sched.wait === "function") {
        __timersPendingForTest.n++;
        return sched.wait(ms, signal ? {
            signal
        } : undefined).then(()=>{
            __timersPendingForTest.n--;
        }, (e)=>{
            __timersPendingForTest.n--;
            throw e;
        });
    }
    __timersPendingForTest.n++;
    return new Promise((resolve, reject)=>{
        let settled = false;
        const done = (fn)=>{
            if (settled) return;
            settled = true;
            __timersPendingForTest.n--;
            fn();
        };
        const t = setTimeout(()=>done(resolve), ms);
        signal?.addEventListener("abort", ()=>{
            clearTimeout(t);
            done(()=>reject(signal.reason ?? new Error("aborted")));
        }, {
            once: true
        });
    });
}
export function withTimeout(p, ms, what) {
    const ac = new AbortController();
    const timer = sleep(ms, ac.signal).then(()=>{
        throw new RpcTimeoutError(what);
    });
    return Promise.race([
        p,
        timer
    ]).finally(()=>ac.abort());
}
export const __timersPendingForTest = {
    n: 0
};
const indexInflight = new Map();
export function fetchRouteIndexDedup(digest, fetcher) {
    const existing = indexInflight.get(digest);
    if (existing) return existing;
    const p = (async ()=>parseIndex(await fetcher()))();
    indexInflight.set(digest, p);
    const cleanup = ()=>{
        if (indexInflight.get(digest) === p) indexInflight.delete(digest);
    };
    p.then(cleanup, cleanup);
    return p;
}
export function __cacheInvariantForTest() {
    let sum = 0;
    for (const slot of indexCache.values())sum += entryBytes(slot.entry);
    return {
        entries: indexCache.size,
        bytes: indexCacheBytes,
        consistent: sum === indexCacheBytes
    };
}
export const ESCAPE = /[-/\\^$+?.()|[\]{}]/g;
export function globMatch(rules, path) {
    for (const re of rules){
        if (re.test(path)) return true;
    }
    return false;
}
export function decodeSegment(seg) {
    try {
        return decodeURIComponent(seg);
    } catch  {
        return seg;
    }
}
export function normalizePath(raw) {
    if (!raw || raw.length > 2048 || raw.includes("\\") || raw.includes("\0")) return null;
    let out = raw.startsWith("/") ? "" : "/";
    const trailing = raw.endsWith("/") && raw.length > 1;
    const parts = raw.split("/").filter((p)=>p !== "");
    for(let i = 0; i < parts.length; i++){
        const part = decodeSegment(parts[i]);
        if (part === "." || part === ".." || part.includes("\\") || part.includes("\0")) {
            return null;
        }
        if (i > 0 || !out.endsWith("/")) out += "/";
        out += part;
    }
    if (trailing && !out.endsWith("/")) out += "/";
    return out === "" ? "/" : out;
}
function htmlKey(path) {
    if (path === "/") return "/";
    let key;
    if (path.endsWith("/index.html")) key = path.slice(0, -"/index.html".length);
    else if (path.endsWith("/index")) key = path.slice(0, -"/index".length);
    else if (path.endsWith(".html")) key = path.slice(0, -".html".length);
    else if (path.endsWith("/")) key = path.slice(0, -1);
    else {
        const last = path.slice(path.lastIndexOf("/") + 1);
        if (last.includes(".")) return null;
        key = path;
    }
    return key === "" ? "/" : key;
}
export function intentFetchable(index, path, navigate, hasStaticRouting) {
    const has = (p)=>index.paths.has(p);
    if (has(path)) return true;
    const nfh = hasStaticRouting || navigate ? index.notFoundHandling : "none";
    if (index.htmlHandling !== "none") {
        const key = htmlKey(path);
        if (key !== null) {
            if (key === "/" ? has("/index.html") : has(`${key}.html`) || has(`${key}/index.html`)) return true;
        }
    }
    if (nfh === "single-page-application") return has("/index.html");
    if (nfh === "404-page") {
        let dir = path;
        while(dir.length > 0){
            const idx = dir.lastIndexOf("/");
            dir = idx <= 0 ? "" : dir.slice(0, idx);
            if (has(`${dir === "" ? "" : dir}/404.html`)) return true;
        }
        return false;
    }
    return false;
}
const MAX_CFG_ENTRIES = 16;
const cfgCache = new Map();
let cfgCompiles = 0;
function compileRule(rule) {
    try {
        return new RegExp("^" + rule.split("*").map((s)=>s.replace(ESCAPE, "\$&")).join(".*") + "$");
    } catch  {
        return undefined;
    }
}
export function compileCfg(raw) {
    if (cfgCache.has(raw)) return cfgCache.get(raw);
    cfgCompiles++;
    const compiled = (()=>{
        let cfg;
        try {
            cfg = JSON.parse(raw);
        } catch  {
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
        if (userWorker.some((r)=>typeof r !== "string" || r.length > 512) || assetWorker.some((r)=>typeof r !== "string" || r.length > 512)) {
            return undefined;
        }
        return {
            invokeAhead: cfg.invokeUserWorkerAhead === true,
            userWorker: userWorker.map(compileRule).filter((r)=>!!r),
            assetWorker: assetWorker.map(compileRule).filter((r)=>!!r),
            manifestDigest: digest
        };
    })();
    cfgCache.set(raw, compiled);
    if (cfgCache.size > MAX_CFG_ENTRIES) {
        const oldest = cfgCache.keys().next();
        if (!oldest.done) cfgCache.delete(oldest.value);
    }
    return compiled;
}
const HTML_HANDLINGS = new Set([
    "auto-trailing-slash",
    "force-trailing-slash",
    "drop-trailing-slash",
    "none"
]);
const NOT_FOUND_HANDLINGS = new Set([
    "none",
    "404-page",
    "single-page-application"
]);
const DIGEST_HEX = /^[0-9a-f]{64}$/;
export function parseIndex(raw) {
    if (typeof raw !== "object" || raw === null) return {
        kind: "invalid"
    };
    const doc = raw;
    if (typeof doc.htmlHandling !== "string" || !HTML_HANDLINGS.has(doc.htmlHandling) || typeof doc.notFoundHandling !== "string" || !NOT_FOUND_HANDLINGS.has(doc.notFoundHandling)) {
        return {
            kind: "invalid"
        };
    }
    if (typeof doc.manifestDigest !== "string" || !DIGEST_HEX.test(doc.manifestDigest)) {
        return {
            kind: "invalid"
        };
    }
    const md = doc.manifestDigest;
    if (doc.tooLarge === true) return {
        kind: "tooLarge"
    };
    if (!Array.isArray(doc.paths)) return {
        kind: "invalid"
    };
    if (doc.paths.length > MAX_INDEX_PATHS) return {
        kind: "tooLarge"
    };
    const paths = new Set();
    let approxBytes = 64;
    for (const p of doc.paths){
        if (typeof p !== "string" || p.length > 2048) return {
            kind: "invalid"
        };
        paths.add(p);
        approxBytes += p.length + 8;
    }
    if (approxBytes > MAX_INDEX_BYTES) return {
        kind: "tooLarge"
    };
    return {
        kind: "index",
        index: {
            htmlHandling: doc.htmlHandling,
            notFoundHandling: doc.notFoundHandling,
            paths,
            approxBytes,
            digest: "",
            manifestDigest: md
        }
    };
}
export function __cfgCompileCountForTest() {
    return cfgCompiles;
}
