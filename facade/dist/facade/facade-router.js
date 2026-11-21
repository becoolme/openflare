import { WorkerEntrypoint } from "cloudflare:workers";
import { compileCfg, cacheGet, cachePut, fetchRouteIndexDedup, normalizePath, intentFetchable, globMatch, withTimeout, RpcTimeoutError, RPC_BUDGET } from "./router-core.js";
import { ROUTE_INDEX_REQUEST, canFetchInput, ROUTE_ERROR_HEADER } from "./router-wire.js";
const ASSET_ROUTE_HEADER = "x-vrt-asset-route";
function routeError(code, digest, err) {
    const name = err instanceof Error ? err.name : typeof err;
    const detail = err instanceof Error && "code" in err && typeof err.code === "string" ? err.code : "";
    console.error(JSON.stringify({
        code: code.toUpperCase(),
        digestPrefix: digest.slice(0, 12),
        error: name,
        detail
    }));
}
function stripMarker(response) {
    const hasMarker = response.headers.has(ASSET_ROUTE_HEADER) || response.headers.has(ROUTE_ERROR_HEADER);
    if (!hasMarker) return response;
    const headers = new Headers(response.headers);
    headers.delete(ASSET_ROUTE_HEADER);
    headers.delete(ROUTE_ERROR_HEADER);
    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers
    });
}
function marked(response) {
    const headers = new Headers(response.headers);
    headers.set(ASSET_ROUTE_HEADER, "assets");
    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers
    });
}
const ROUTE_UNREACHABLE = (reason)=>{
    const headers = new Headers({
        "content-type": "application/json",
        [ASSET_ROUTE_HEADER]: "assets",
        [ROUTE_ERROR_HEADER]: reason
    });
    return new Response(JSON.stringify({
        error: {
            code: "ASSETS_ROUTE_UNAVAILABLE"
        }
    }), {
        status: 503,
        headers
    });
};
export class AssetsRouter extends WorkerEntrypoint {
    async fetch(request) {
        const env = this.env;
        const cfg = compileCfg(env.VRT_ROUTER_CFG ?? "");
        if (cfg === undefined) {
            routeError("route_index_invalid", "", new Error(`VRT_ROUTER_CFG failed compile (len=${env.VRT_ROUTER_CFG?.length ?? 0})`));
            return ROUTE_UNREACHABLE("route_index_invalid");
        }
        const hasStaticRouting = cfg.userWorker.length + cfg.assetWorker.length > 0;
        const toUserWorker = async ()=>stripMarker(await env.USER_WORKER.fetch(request));
        const toAssets = async ()=>marked(await env.ASSETS.fetch(request));
        if (hasStaticRouting) {
            const pathname = new URL(request.url).pathname;
            if (globMatch(cfg.assetWorker, pathname)) return toAssets();
            if (globMatch(cfg.userWorker, pathname)) return toUserWorker();
        }
        if (cfg.invokeAhead) return toUserWorker();
        const method = request.method.toUpperCase();
        if (method !== "GET" && method !== "HEAD") return toUserWorker();
        const rawPath = new URL(request.url).pathname;
        const pathname = normalizePath(rawPath);
        if (pathname === null) return toUserWorker();
        const navigate = request.headers.get("sec-fetch-mode") === "navigate";
        const fetchable = await this.#fetchable(request, pathname, rawPath, navigate, hasStaticRouting, cfg.manifestDigest);
        if (fetchable.error) return ROUTE_UNREACHABLE(fetchable.error);
        return fetchable.value ? toAssets() : toUserWorker();
    }
    async #fetchable(request, pathname, rawPath, navigate, hasStaticRouting, digest) {
        const env = this.env;
        let slot = cacheGet(digest);
        if (slot === undefined) {
            let parsed;
            try {
                parsed = await withTimeout(fetchRouteIndexDedup(digest, ()=>env.ASSETS.fetchRouteIndex(ROUTE_INDEX_REQUEST)), RPC_BUDGET.indexMs, "route-index");
            } catch (err) {
                const reason = err instanceof RpcTimeoutError ? "route_index_timeout" : "route_index_rpc_failed";
                routeError(reason, digest, err);
                return {
                    value: false,
                    error: reason
                };
            }
            if (parsed.kind === "invalid") {
                routeError("route_index_invalid", digest, new Error("route-index response failed wire validation"));
                return {
                    value: false,
                    error: "route_index_invalid"
                };
            }
            if (parsed.kind === "tooLarge") {
                cachePut({
                    kind: "tooLarge",
                    digest,
                    approxBytes: 8
                });
                return this.#probe(request, hasStaticRouting, digest);
            }
            if (parsed.index.manifestDigest !== digest) {
                routeError("route_index_digest_mismatch", digest, new Error(`index declared ${parsed.index.manifestDigest.slice(0, 12)}`));
                return {
                    value: false,
                    error: "route_index_digest_mismatch"
                };
            }
            const index = parsed.index;
            index.digest = digest;
            slot = {
                kind: "index",
                index
            };
            cachePut(slot);
        }
        if (slot.kind === "tooLarge") {
            return this.#probe(request, hasStaticRouting, digest);
        }
        if (slot.kind !== "index") {
            routeError("route_index_invalid", digest, new Error("unreachable cache slot kind"));
            return {
                value: false,
                error: "route_index_invalid"
            };
        }
        return {
            value: intentFetchable(slot.index, pathname, navigate, hasStaticRouting, rawPath)
        };
    }
    async #probe(request, hasStaticRouting, digest) {
        try {
            return {
                value: await withTimeout(this.env.ASSETS.canFetchAsset(canFetchInput(request, hasStaticRouting)), RPC_BUDGET.probeMs, "can-fetch")
            };
        } catch (err) {
            const reason = err instanceof RpcTimeoutError ? "can_fetch_timeout" : "can_fetch_rpc_failed";
            routeError(reason, digest, err);
            return {
                value: false,
                error: reason
            };
        }
    }
}
export default AssetsRouter;
