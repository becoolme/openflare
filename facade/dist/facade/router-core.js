export const MAX_INDEX_ENTRIES = 8;
export const MAX_INDEX_BYTES = 4 * 1024 * 1024;
export const MAX_INDEX_PATHS = 4096;
export const MAX_REDIRECTS = 2100;
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
    if (out === "") out = "/";
    while(out.includes("//"))out = out.replaceAll("//", "/");
    return out;
}
function findAsset(has, path) {
    return has(path) ? path : null;
}
function safeRedirect(has, mode, file, dest, skip) {
    if (skip || dest === "" || has(dest)) return null;
    const target = findAsset(has, file);
    if (target === null) return null;
    const intent = htmlIntent(has, mode, dest, true);
    return intent !== null && "asset" in intent && intent.asset === target ? dest : null;
}
function htmlIntent(has, mode, path, skip) {
    if (mode === "none") {
        const asset = findAsset(has, path);
        return asset === null ? null : {
            asset
        };
    }
    if (mode === "force-trailing-slash") return htmlForce(has, path, skip);
    if (mode === "drop-trailing-slash") return htmlDrop(has, path, skip);
    return htmlAuto(has, path, skip);
}
function htmlAuto(has, path, skip) {
    const exact = findAsset(has, path);
    if (path.endsWith("/index")) {
        if (exact !== null) return {
            asset: exact
        };
        const dir = path.slice(0, -"index".length);
        const first = safeRedirect(has, "auto-trailing-slash", `${path}.html`, dir, skip);
        if (first !== null) return {
            redirect: first
        };
        const stem = dir.replace(/\/+$/, "");
        const second = safeRedirect(has, "auto-trailing-slash", `${stem}.html`, stem, skip);
        if (second !== null) return {
            redirect: second
        };
    } else if (path.endsWith("/index.html")) {
        const dir = path.slice(0, -"index.html".length);
        const first = safeRedirect(has, "auto-trailing-slash", path, dir, skip);
        if (first !== null) return {
            redirect: first
        };
        const stem = dir.replace(/\/+$/, "");
        const second = safeRedirect(has, "auto-trailing-slash", `${stem}.html`, stem, skip);
        if (second !== null) return {
            redirect: second
        };
    } else if (path.endsWith("/")) {
        const index = findAsset(has, `${path}index.html`);
        if (index !== null) return {
            asset: index
        };
        const stem = path.replace(/\/+$/, "");
        const target = safeRedirect(has, "auto-trailing-slash", `${stem}.html`, stem, skip);
        if (target !== null) return {
            redirect: target
        };
    } else if (path.endsWith(".html")) {
        const stem = path.slice(0, -".html".length);
        const first = safeRedirect(has, "auto-trailing-slash", path, stem, skip);
        if (first !== null) return {
            redirect: first
        };
        const second = safeRedirect(has, "auto-trailing-slash", `${stem}/index.html`, `${stem}/`, skip);
        if (second !== null) return {
            redirect: second
        };
    }
    if (exact !== null) return {
        asset: exact
    };
    const html = findAsset(has, `${path}.html`);
    if (html !== null) return {
        asset: html
    };
    const tail = safeRedirect(has, "auto-trailing-slash", `${path}/index.html`, `${path}/`, skip);
    return tail === null ? null : {
        redirect: tail
    };
}
function htmlForce(has, path, skip) {
    const exact = findAsset(has, path);
    if (path.endsWith("/index")) {
        if (exact !== null) return {
            asset: exact
        };
        const dir = path.slice(0, -"index".length);
        const first = safeRedirect(has, "force-trailing-slash", `${path}.html`, dir, skip);
        if (first !== null) return {
            redirect: first
        };
        const stem = dir.replace(/\/+$/, "");
        const second = safeRedirect(has, "force-trailing-slash", `${stem}.html`, dir, skip);
        if (second !== null) return {
            redirect: second
        };
    } else if (path.endsWith("/index.html")) {
        const dir = path.slice(0, -"index.html".length);
        const first = safeRedirect(has, "force-trailing-slash", path, dir, skip);
        if (first !== null) return {
            redirect: first
        };
        const stem = dir.replace(/\/+$/, "");
        const second = safeRedirect(has, "force-trailing-slash", `${stem}.html`, dir, skip);
        if (second !== null) return {
            redirect: second
        };
    } else if (path.endsWith("/")) {
        const index = findAsset(has, `${path}index.html`);
        if (index !== null) return {
            asset: index
        };
        const stem = path.replace(/\/+$/, "");
        const html = findAsset(has, `${stem}.html`);
        if (html !== null) return {
            asset: html
        };
    } else if (path.endsWith(".html")) {
        const slashed = `${path.slice(0, -".html".length)}/`;
        const first = safeRedirect(has, "force-trailing-slash", path, slashed, skip);
        if (first !== null) return {
            redirect: first
        };
        if (exact !== null) return {
            asset: exact
        };
        const second = safeRedirect(has, "force-trailing-slash", `${slashed}index.html`, slashed, skip);
        if (second !== null) return {
            redirect: second
        };
    }
    if (exact !== null) return {
        asset: exact
    };
    const slashed = `${path}/`;
    const first = safeRedirect(has, "force-trailing-slash", `${path}.html`, slashed, skip);
    if (first !== null) return {
        redirect: first
    };
    const second = safeRedirect(has, "force-trailing-slash", `${path}/index.html`, slashed, skip);
    return second === null ? null : {
        redirect: second
    };
}
function htmlDrop(has, path, skip) {
    const exact = findAsset(has, path);
    if (path.endsWith("/index")) {
        if (exact !== null) return {
            asset: exact
        };
        const stem = path.slice(0, -"/index".length);
        if (stem === "") {
            const root = safeRedirect(has, "drop-trailing-slash", "/index.html", "/", skip);
            if (root !== null) return {
                redirect: root
            };
        } else {
            const first = safeRedirect(has, "drop-trailing-slash", `${stem}.html`, stem, skip);
            if (first !== null) return {
                redirect: first
            };
            const second = safeRedirect(has, "drop-trailing-slash", `${path}.html`, stem, skip);
            if (second !== null) return {
                redirect: second
            };
        }
    } else if (path.endsWith("/index.html")) {
        if (path === "/index.html") {
            const root = safeRedirect(has, "drop-trailing-slash", "/index.html", "/", skip);
            if (root !== null) return {
                redirect: root
            };
        } else {
            const stem = path.slice(0, -"/index.html".length);
            const first = safeRedirect(has, "drop-trailing-slash", path, stem, skip);
            if (first !== null) return {
                redirect: first
            };
            if (exact !== null) return {
                asset: exact
            };
            const second = safeRedirect(has, "drop-trailing-slash", `${stem}.html`, stem, skip);
            if (second !== null) return {
                redirect: second
            };
        }
    } else if (path.endsWith("/")) {
        if (path === "/") {
            const index = findAsset(has, "/index.html");
            if (index !== null) return {
                asset: index
            };
        } else {
            const stem = path.replace(/\/+$/, "");
            const first = safeRedirect(has, "drop-trailing-slash", `${stem}.html`, stem, skip);
            if (first !== null) return {
                redirect: first
            };
            const second = safeRedirect(has, "drop-trailing-slash", `${stem}/index.html`, stem, skip);
            if (second !== null) return {
                redirect: second
            };
        }
    } else if (path.endsWith(".html")) {
        const stem = path.slice(0, -".html".length);
        const first = safeRedirect(has, "drop-trailing-slash", path, stem, skip);
        if (first !== null) return {
            redirect: first
        };
        const second = safeRedirect(has, "drop-trailing-slash", `${stem}/index.html`, stem, skip);
        if (second !== null) return {
            redirect: second
        };
    }
    if (exact !== null) return {
        asset: exact
    };
    const html = findAsset(has, `${path}.html`);
    if (html !== null) return {
        asset: html
    };
    const index = findAsset(has, `${path}/index.html`);
    return index === null ? null : {
        asset: index
    };
}
function redirectMatches(rules, path) {
    for (const rule of rules){
        if (rule.source === path) return true;
        if (!rule.source.includes("*") && !rule.source.includes(":")) continue;
        const src = rule.source.split("/");
        const req = path.split("/");
        let matched = true;
        for(let i = 0; i < src.length; i++){
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
export function intentFetchable(index, path, navigate, hasStaticRouting, rawPath = path) {
    const has = (p)=>index.paths.has(p);
    if (has(path)) return true;
    if (redirectMatches(index.redirects, rawPath)) return true;
    const nfh = hasStaticRouting || navigate ? index.notFoundHandling : "none";
    if (htmlIntent(has, index.htmlHandling, path, false) !== null) return true;
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
    const redirects = [];
    if (doc.redirects !== undefined) {
        if (!Array.isArray(doc.redirects) || doc.redirects.length > MAX_REDIRECTS) {
            return {
                kind: "invalid"
            };
        }
        for (const r of doc.redirects){
            if (typeof r !== "object" || r === null) return {
                kind: "invalid"
            };
            const { source, status } = r;
            if (typeof source !== "string" || !source.startsWith("/") || source.length > 1024 || status !== 200 && status !== 301 && status !== 302 && status !== 303 && status !== 307 && status !== 308) {
                return {
                    kind: "invalid"
                };
            }
            redirects.push({
                source,
                status
            });
            approxBytes += source.length + 16;
        }
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
            redirects,
            approxBytes,
            digest: "",
            manifestDigest: md
        }
    };
}
export function __cfgCompileCountForTest() {
    return cfgCompiles;
}
