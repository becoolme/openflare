// facade-kv system worker entry: vendored KVNamespace entrypoint plus the
// vrt props → identity-header adapter (see props.ts). This VRT subclass also
// adds a bounded process-local read cache for explicit `cacheTtl` reads; it is
// not the distributed Cloudflare edge cache.
import { KVNamespace as VendoredKVNamespace } from "../third_party/open-compute/facade/kv/transport.js";
import { envWithBackend } from "./props.js";

interface CacheEntry {
  expires: number;
  value?: unknown;
  text?: string;
  metadata?: string;
}

const MAX_CACHE_ENTRIES = 4096;
const cache = new Map<string, CacheEntry>();

function cacheTtl(options: unknown): number | undefined {
  if (options === null || typeof options !== "object") return undefined;
  const v = (options as { cacheTtl?: unknown }).cacheTtl;
  return typeof v === "number" ? v : undefined;
}

function readType(options: unknown): "text" | "json" | "arrayBuffer" | "stream" {
  if (typeof options === "string") return options as never;
  if (options !== null && typeof options === "object") {
    const t = (options as { type?: unknown }).type;
    if (t === "json" || t === "arrayBuffer" || t === "stream") return t;
  }
  return "text";
}

function cacheable(
  options: unknown,
  ttl: number | undefined,
  type: "text" | "json" | "arrayBuffer" | "stream",
): boolean {
  if (ttl === undefined || !Number.isSafeInteger(ttl) || ttl < 30) return false;
  if (type === "stream") return false;
  if (typeof options === "string") return true;
  if (options === null || typeof options !== "object") return false;
  const declared = (options as { type?: unknown }).type;
  return declared === undefined || declared === type;
}

function remember(key: string, entry: CacheEntry): void {
  if (cache.size >= MAX_CACHE_ENTRIES) {
    const first = cache.keys().next();
    if (!first.done) cache.delete(first.value);
  }
  cache.set(key, entry);
}

function hit(key: string, type: "text" | "json" | "arrayBuffer"): unknown | undefined {
  const entry = cache.get(key);
  if (entry === undefined) return undefined;
  if (entry.expires <= Date.now()) {
    cache.delete(key);
    return undefined;
  }
  cache.delete(key);
  cache.set(key, entry);
  if (type === "arrayBuffer") return (entry.value as ArrayBuffer).slice(0);
  if (type === "json") return JSON.parse(entry.text!);
  return entry.text;
}

export class KVNamespace extends VendoredKVNamespace {
  readonly #scope: string;

  constructor(ctx: { props?: unknown }, env: unknown) {
    super(ctx, envWithBackend(env, ctx?.props));
    const p = (ctx?.props ?? {}) as { instanceId?: unknown; namespaceResourceId?: unknown };
    this.#scope = `${String(p.instanceId ?? "")}\u0000${String(p.namespaceResourceId ?? "")}`;
  }

  async get(keyOrKeys: string | string[], typeOrOptions?: unknown): Promise<unknown> {
    const ttl = cacheTtl(typeOrOptions);
    const type = readType(typeOrOptions);
    if (Array.isArray(keyOrKeys) || !cacheable(typeOrOptions, ttl, type)) {
      return super.get(keyOrKeys as never, typeOrOptions as never);
    }
    const key = `${this.#scope}\u0001get\u0001${type}\u0001${keyOrKeys}`;
    const cached = hit(key, type);
    if (cached !== undefined) return cached;
    const value = await super.get(keyOrKeys, typeOrOptions as never);
    if (value === null) return value;
    if (type === "arrayBuffer")
      remember(key, { expires: Date.now() + ttl * 1000, value: value as ArrayBuffer });
    else if (type === "json")
      remember(key, { expires: Date.now() + ttl * 1000, text: JSON.stringify(value) });
    else remember(key, { expires: Date.now() + ttl * 1000, text: String(value) });
    return value;
  }

  async getWithMetadata(
    keyOrKeys: string | string[],
    typeOrOptions?: unknown,
  ): Promise<unknown> {
    const ttl = cacheTtl(typeOrOptions);
    const type = readType(typeOrOptions);
    if (Array.isArray(keyOrKeys) || !cacheable(typeOrOptions, ttl, type)) {
      return super.getWithMetadata(keyOrKeys as never, typeOrOptions as never);
    }
    const key = `${this.#scope}\u0001meta\u0001${type}\u0001${keyOrKeys}`;
    const entry = cache.get(key);
    if (entry !== undefined && entry.expires > Date.now()) {
      cache.delete(key);
      cache.set(key, entry);
      if (type === "arrayBuffer")
        return {
          value: (entry.value as ArrayBuffer).slice(0),
          metadata: entry.metadata === undefined ? null : JSON.parse(entry.metadata),
        };
      return {
        value: type === "json" ? JSON.parse(entry.text!) : entry.text,
        metadata: entry.metadata === undefined ? null : JSON.parse(entry.metadata),
      };
    }
    if (entry !== undefined) cache.delete(key);
    const result = (await super.getWithMetadata(
      keyOrKeys,
      typeOrOptions as never,
    )) as { value: unknown; metadata: unknown };
    if (result.value === null) return result;
    remember(key, {
      expires: Date.now() + ttl * 1000,
      ...(type === "arrayBuffer"
        ? { value: result.value as ArrayBuffer }
        : {
            text:
              type === "json"
                ? JSON.stringify(result.value)
                : String(result.value),
          }),
      ...(result.metadata === null ? {} : { metadata: JSON.stringify(result.metadata) }),
    });
    return result;
  }

  async put(key: string, value: unknown, options?: unknown): Promise<void> {
    await super.put(key, value, options as never);
    this.#invalidate(key);
  }

  async delete(key: string): Promise<void> {
    await super.delete(key);
    this.#invalidate(key);
  }

  #invalidate(key: string): void {
    const prefix = `${this.#scope}\u0001`;
    for (const cacheKey of cache.keys()) {
      if (cacheKey.startsWith(prefix) && cacheKey.endsWith(`\u0001${key}`)) cache.delete(cacheKey);
    }
  }
}
export default KVNamespace;
