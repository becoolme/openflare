import { KVNamespace as VendoredKVNamespace } from "../third_party/open-compute/facade/kv/transport.js";
import { envWithBackend } from "./props.js";
const MAX_CACHE_ENTRIES = 4096;
const cache = new Map();
function cacheTtl(options) {
    if (options === null || typeof options !== "object") return undefined;
    const v = options.cacheTtl;
    return typeof v === "number" ? v : undefined;
}
function readType(options) {
    if (typeof options === "string") return options;
    if (options !== null && typeof options === "object") {
        const t = options.type;
        if (t === "json" || t === "arrayBuffer" || t === "stream") return t;
    }
    return "text";
}
function remember(key, entry) {
    if (cache.size >= MAX_CACHE_ENTRIES) {
        const first = cache.keys().next();
        if (!first.done) cache.delete(first.value);
    }
    cache.set(key, entry);
}
function hit(key, type) {
    const entry = cache.get(key);
    if (entry === undefined) return undefined;
    if (entry.expires <= Date.now()) {
        cache.delete(key);
        return undefined;
    }
    cache.delete(key);
    cache.set(key, entry);
    if (type === "arrayBuffer") return entry.value.slice(0);
    if (type === "json") return JSON.parse(entry.text);
    return entry.text;
}
export class KVNamespace extends VendoredKVNamespace {
    #scope;
    constructor(ctx, env){
        super(ctx, envWithBackend(env, ctx?.props));
        const p = ctx?.props ?? {};
        this.#scope = `${String(p.instanceId ?? "")}\u0000${String(p.namespaceResourceId ?? "")}`;
    }
    async get(keyOrKeys, typeOrOptions) {
        const ttl = cacheTtl(typeOrOptions);
        const type = readType(typeOrOptions);
        if (Array.isArray(keyOrKeys) || ttl === undefined || ttl < 30 || type === "stream") {
            return super.get(keyOrKeys, typeOrOptions);
        }
        const key = `${this.#scope}\u0001get\u0001${type}\u0001${keyOrKeys}`;
        const cached = hit(key, type);
        if (cached !== undefined) return cached;
        const value = await super.get(keyOrKeys, typeOrOptions);
        if (value === null) return value;
        if (type === "arrayBuffer") remember(key, {
            expires: Date.now() + ttl * 1000,
            value: value
        });
        else if (type === "json") remember(key, {
            expires: Date.now() + ttl * 1000,
            text: JSON.stringify(value)
        });
        else remember(key, {
            expires: Date.now() + ttl * 1000,
            text: String(value)
        });
        return value;
    }
    async getWithMetadata(keyOrKeys, typeOrOptions) {
        const ttl = cacheTtl(typeOrOptions);
        const type = readType(typeOrOptions);
        if (Array.isArray(keyOrKeys) || ttl === undefined || ttl < 30 || type === "stream") {
            return super.getWithMetadata(keyOrKeys, typeOrOptions);
        }
        const key = `${this.#scope}\u0001meta\u0001${type}\u0001${keyOrKeys}`;
        const entry = cache.get(key);
        if (entry !== undefined && entry.expires > Date.now()) {
            cache.delete(key);
            cache.set(key, entry);
            if (type === "arrayBuffer") return {
                value: entry.value.slice(0),
                metadata: entry.metadata === undefined ? null : JSON.parse(entry.metadata)
            };
            return {
                value: type === "json" ? JSON.parse(entry.text) : entry.text,
                metadata: entry.metadata === undefined ? null : JSON.parse(entry.metadata)
            };
        }
        if (entry !== undefined) cache.delete(key);
        const result = await super.getWithMetadata(keyOrKeys, typeOrOptions);
        if (result.value === null) return result;
        remember(key, {
            expires: Date.now() + ttl * 1000,
            ...type === "arrayBuffer" ? {
                value: result.value
            } : {
                text: type === "json" ? JSON.stringify(result.value) : String(result.value)
            },
            ...result.metadata === null ? {} : {
                metadata: JSON.stringify(result.metadata)
            }
        });
        return result;
    }
    async put(key, value, options) {
        await super.put(key, value, options);
        this.#invalidate(key);
    }
    async delete(key) {
        await super.delete(key);
        this.#invalidate(key);
    }
    #invalidate(key) {
        const prefix = `${this.#scope}\u0001`;
        for (const cacheKey of cache.keys()){
            if (cacheKey.startsWith(prefix) && cacheKey.endsWith(`\u0001${key}`)) cache.delete(cacheKey);
        }
    }
}
export default KVNamespace;
