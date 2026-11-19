// MIG-16: `ratelimits` binding facade. The tenant binding is a plain service
// binding to this entrypoint; `env.X.limit({ key })` is a WorkerEntrypoint RPC.
// Identity/limits come ONLY from host-injected ctx.props — a tenant can pass
// nothing but `key`. Counter scope = tenant instance + worker + namespace_id,
// so two tenants (or workers) never share a counter.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  RateLimitStore,
  ratelimitParams,
  validateRateLimitKey,
  type RateLimitParams,
} from "./ratelimit-core.js";

const store = new RateLimitStore();
export class RateLimiter extends WorkerEntrypoint {
  readonly #params: RateLimitParams;
  constructor(ctx: { props?: unknown }, env: unknown) {
    super(ctx as never, env as never);
    this.#params = ratelimitParams(ctx?.props);
  }

  async limit(options: unknown): Promise<{ success: boolean }> {
    const key = validateRateLimitKey(options);
    return { success: store.hit(this.#params, key) };
  }
}
export default RateLimiter;
