// MIG-16: in-process fixed-window rate-limit counter shared by the
// `facade-ratelimit` system worker. Pure logic (injectable clock) so it is
// unit-testable without workerd.
//
// Semantics (Cloudflare Workers Rate Limiting compatible, approximate):
//  - fixed windows aligned to `period` seconds since the epoch;
//  - the first `limit` calls in a window succeed, later ones return false;
//  - counters are local to this workerd process (CF itself is per-colo
//    approximate) — with N replicas the effective cap is ~limit x N.
//
// Memory is bounded: at most `maxEntries` counters globally and
// `maxPerScope` per scope. On insert at most SWEEP_BATCH oldest entries are
// examined; expired ones are dropped, else the oldest live counter is evicted
// (that key's count resets: fail-open for that key only, never a throw).
// A scope over its own cap only ever evicts its OWN entries, so one noisy
// tenant/worker cannot reset another scope's counters.

import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";

export const RATELIMIT_KEY_MAX_BYTES = 1024;
export const RATELIMIT_MAX_ENTRIES = 100_000;
/** Per-scope (tenant+worker+namespace) live-counter cap. */
export const RATELIMIT_MAX_PER_SCOPE = 10_000;
/** Max entries EXAMINED per insert (keeps every call O(1)). */
const SWEEP_BATCH = 64;

interface Counter {
  windowStart: number;
  count: number;
  periodMs: number;
}

export interface RateLimitParams {
  /** Scope string; must already combine tenant + worker + namespace. */
  readonly scope: string;
  readonly limit: number;
  readonly periodSec: number;
}

export class RateLimitStore {
  // scope -> (key -> counter). Both levels keep insertion order, so the first
  // entry is always the oldest one. Empty scope maps are removed.
  private readonly scopes = new Map<string, Map<string, Counter>>();
  private total = 0;
  /** Entries examined by the most recent makeRoom (test/diagnostic only). */
  lastMakeRoomChecks = 0;
  constructor(
    private readonly now: () => number = Date.now,
    private readonly maxEntries: number = RATELIMIT_MAX_ENTRIES,
    private readonly maxPerScope: number = RATELIMIT_MAX_PER_SCOPE,
  ) {}

  get size(): number {
    return this.total;
  }

  /** Returns true when the call is within the limit. */
  hit(params: RateLimitParams, key: string): boolean {
    const periodMs = params.periodSec * 1000;
    const t = this.now();
    const windowStart = Math.floor(t / periodMs) * periodMs;
    const { scope } = params;
    const cur = this.scopes.get(scope)?.get(key);
    if (cur !== undefined && cur.windowStart === windowStart) {
      if (cur.count >= params.limit) return false;
      cur.count += 1;
      return true;
    }
    // New key or rolled-over window: (re)insert at the young end.
    if (cur !== undefined) this.remove(scope, key);
    else this.makeRoom(scope, t);
    let m = this.scopes.get(scope);
    if (m === undefined) {
      m = new Map();
      this.scopes.set(scope, m);
    }
    m.set(key, { windowStart, count: 1, periodMs });
    this.total += 1;
    return true;
  }

  private remove(scope: string, key: string): void {
    const m = this.scopes.get(scope);
    if (m === undefined || !m.delete(key)) return;
    this.total -= 1;
    if (m.size === 0) this.scopes.delete(scope);
  }

  /**
   * Free at most one slot for `scope`. Work is bounded: at most SWEEP_BATCH
   * entries are *examined* (not deleted) per call.
   *  1. scope at its own cap  -> sweep/evict only inside that scope;
   *  2. global table full     -> sweep/evict in the oldest scope (rotated to
   *     the back so pressure is spread).
   */
  private makeRoom(scope: string, t: number): void {
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
      this.scopes.set(victim, m); // rotate
    }
  }

  private reclaim(scope: string, t: number): void {
    const m = this.scopes.get(scope);
    if (m === undefined) return;
    const expired: string[] = [];
    let checks = 0;
    for (const [key, c] of m) {
      if (checks >= SWEEP_BATCH) break;
      checks += 1;
      if (c.windowStart + c.periodMs <= t) expired.push(key);
    }
    this.lastMakeRoomChecks += checks;
    if (expired.length > 0) {
      for (const k of expired) this.remove(scope, k);
      return;
    }
    // Nothing expired among the examined oldest entries: evict the oldest
    // live counter (that key's count resets: fail-open for it only).
    const oldest = m.keys().next();
    if (!oldest.done) this.remove(scope, oldest.value);
  }
}

export function validateRateLimitKey(options: unknown): string {
  if (options === null || typeof options !== "object") {
    throw new TypeError("limit() expects an options object");
  }
  const key = (options as { key?: unknown }).key;
  if (typeof key !== "string") {
    throw new TypeError("limit() options.key must be a string");
  }
  if (
    key.length > RATELIMIT_KEY_MAX_BYTES ||
    new TextEncoder().encode(key).byteLength > RATELIMIT_KEY_MAX_BYTES
  ) {
    throw new TypeError(
      `limit() options.key must be at most ${RATELIMIT_KEY_MAX_BYTES} bytes`,
    );
  }
  return key;
}

const ID_RE = /^[A-Za-z0-9._:~-]{1,256}$/;
const NAMESPACE_RE = /^[0-9]{1,32}$/;
export const RATELIMIT_LIMIT_MAX = 1_000_000;

export function ratelimitParams(props: unknown): RateLimitParams {
  if (props === null || typeof props !== "object" || Array.isArray(props)) {
    throw bindingError("BINDING_PROTOCOL_ERROR");
  }
  const p = props as Record<string, unknown>;
  if (
    typeof p.instanceId !== "string" ||
    !ID_RE.test(p.instanceId) ||
    typeof p.workerId !== "string" ||
    !ID_RE.test(p.workerId) ||
    typeof p.namespaceId !== "string" ||
    !NAMESPACE_RE.test(p.namespaceId) ||
    !Number.isSafeInteger(p.limit) ||
    (p.limit as number) < 1 ||
    (p.limit as number) > RATELIMIT_LIMIT_MAX ||
    (p.period !== 10 && p.period !== 60)
  ) {
    throw bindingError("BINDING_PROTOCOL_ERROR");
  }
  return {
    scope: `${p.instanceId}\u0001${p.workerId}\u0001${p.namespaceId}`,
    limit: p.limit as number,
    periodSec: p.period as number,
  };
}
