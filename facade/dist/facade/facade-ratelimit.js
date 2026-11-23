import { WorkerEntrypoint } from "cloudflare:workers";
import { RateLimitStore, ratelimitParams, validateRateLimitOptions } from "./ratelimit-core.js";
const store = new RateLimitStore();
export class RateLimiter extends WorkerEntrypoint {
    #params;
    constructor(ctx, env){
        super(ctx, env);
        this.#params = ratelimitParams(ctx?.props);
    }
    async limit(options) {
        const { key, delta } = validateRateLimitOptions(options);
        return {
            success: store.hit(this.#params, key, delta)
        };
    }
}
export default RateLimiter;
