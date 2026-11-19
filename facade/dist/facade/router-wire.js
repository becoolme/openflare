export const ASSETS_BINDING = "ASSETS";
export const USER_WORKER_BINDING = "USER_WORKER";
export const ROUTER_CFG_BINDING = "VRT_ROUTER_CFG";
export const ROUTE_INDEX_REQUEST = Object.freeze({
    kind: "route-index"
});
export function isRouteIndexRequest(v) {
    return typeof v === "object" && v !== null && v.kind === "route-index";
}
export function canFetchInput(request, hasStaticRouting) {
    return {
        url: request.url,
        method: request.method,
        headers: [
            ...request.headers
        ],
        hasStaticRouting
    };
}
export const ROUTE_ERROR_REASONS = [
    "route_index_rpc_failed",
    "route_index_invalid",
    "route_index_digest_mismatch",
    "route_index_timeout",
    "can_fetch_rpc_failed",
    "can_fetch_timeout"
];
export const ROUTE_ERROR_HEADER = "x-vrt-route-error";
