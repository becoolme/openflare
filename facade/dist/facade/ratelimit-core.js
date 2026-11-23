import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";
export const RATELIMIT_KEY_MAX_BYTES = 1024;
export const RATELIMIT_MAX_ENTRIES = 100_000;
export const RATELIMIT_MAX_PER_SCOPE = 10_000;
const SWEEP_BATCH = 64;
export class RateLimitStore {
    now;
    maxEntries;
    maxPerScope;
    scopes = new Map();
    total = 0;
    lastMakeRoomChecks = 0;
    constructor(now = Date.now, maxEntries = RATELIMIT_MAX_ENTRIES, maxPerScope = RATELIMIT_MAX_PER_SCOPE){
        this.now = now;
        this.maxEntries = maxEntries;
        this.maxPerScope = maxPerScope;
    }
    get size() {
        return this.total;
    }
    hit(params, key, delta = 1) {
        if (!Number.isSafeInteger(delta) || delta < 1) {
            throw new TypeError("limit() delta must be a positive integer");
        }
        const periodMs = params.periodSec * 1000;
        const t = this.now();
        const windowStart = Math.floor(t / periodMs) * periodMs;
        const { scope } = params;
        const cur = this.scopes.get(scope)?.get(key);
        if (cur !== undefined && cur.windowStart === windowStart) {
            if (cur.count + delta > params.limit) return false;
            cur.count += delta;
            return true;
        }
        if (delta > params.limit) return false;
        if (cur !== undefined) this.remove(scope, key);
        else this.makeRoom(scope, t);
        let m = this.scopes.get(scope);
        if (m === undefined) {
            m = new Map();
            this.scopes.set(scope, m);
        }
        m.set(key, {
            windowStart,
            count: delta,
            periodMs
        });
        this.total += 1;
        return true;
    }
    remove(scope, key) {
        const m = this.scopes.get(scope);
        if (m === undefined || !m.delete(key)) return;
        this.total -= 1;
        if (m.size === 0) this.scopes.delete(scope);
    }
    makeRoom(scope, t) {
        this.lastMakeRoomChecks = 0;
        const own = this.scopes.get(scope);
        if (own !== undefined && own.size >= this.maxPerScope) {
            this.reclaim(scope, t);
            return;
        }
        if (this.total < this.maxEntries) return;
        const first = this.scopes.keys().next();
        if (first.done) return;
        const victim = first.value;
        this.reclaim(victim, t);
        const m = this.scopes.get(victim);
        if (m !== undefined) {
            this.scopes.delete(victim);
            this.scopes.set(victim, m);
        }
    }
    reclaim(scope, t) {
        const m = this.scopes.get(scope);
        if (m === undefined) return;
        const expired = [];
        let checks = 0;
        for (const [key, c] of m){
            if (checks >= SWEEP_BATCH) break;
            checks += 1;
            if (c.windowStart + c.periodMs <= t) expired.push(key);
        }
        this.lastMakeRoomChecks += checks;
        if (expired.length > 0) {
            for (const k of expired)this.remove(scope, k);
            return;
        }
        const oldest = m.keys().next();
        if (!oldest.done) this.remove(scope, oldest.value);
    }
}
export function validateRateLimitOptions(options) {
    const key = validateRateLimitKey(options);
    const delta = options.delta;
    if (delta === undefined) return {
        key,
        delta: 1
    };
    if (typeof delta !== "number" || !Number.isSafeInteger(delta) || delta < 1) {
        throw new TypeError("limit() options.delta must be a positive integer");
    }
    return {
        key,
        delta
    };
}
export function validateRateLimitKey(options) {
    if (options === null || typeof options !== "object") {
        throw new TypeError("limit() expects an options object");
    }
    const key = options.key;
    if (typeof key !== "string") {
        throw new TypeError("limit() options.key must be a string");
    }
    if (key.length > RATELIMIT_KEY_MAX_BYTES || new TextEncoder().encode(key).byteLength > RATELIMIT_KEY_MAX_BYTES) {
        throw new TypeError(`limit() options.key must be at most ${RATELIMIT_KEY_MAX_BYTES} bytes`);
    }
    return key;
}
const ID_RE = /^[A-Za-z0-9._:~-]{1,256}$/;
const NAMESPACE_RE = /^[0-9]{1,32}$/;
export const RATELIMIT_LIMIT_MAX = 1_000_000;
export function ratelimitParams(props) {
    if (props === null || typeof props !== "object" || Array.isArray(props)) {
        throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    const p = props;
    if (typeof p.instanceId !== "string" || !ID_RE.test(p.instanceId) || typeof p.workerId !== "string" || !ID_RE.test(p.workerId) || typeof p.namespaceId !== "string" || !NAMESPACE_RE.test(p.namespaceId) || !Number.isSafeInteger(p.limit) || p.limit < 1 || p.limit > RATELIMIT_LIMIT_MAX || p.period !== 10 && p.period !== 60) {
        throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    return {
        scope: `${p.instanceId}\u0001${p.workerId}\u0001${p.namespaceId}`,
        limit: p.limit,
        periodSec: p.period
    };
}
