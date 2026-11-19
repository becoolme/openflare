// Shared typed wire between `facade-router` and `facade-assets` —
// BOTH sides import from this module so the RPC contract cannot
// silently fork (T31f/F0: the router once passed `{}` to
// `fetchRouteIndex`, which demanded a full asset request and threw
// before any backend call; neither side's stub ever executed the
// other's validation).
//
// Binding names are also declared here once: capnp pins them by
// constant (ROUTER_ASSETS_BINDING / ROUTER_USER_WORKER_BINDING /
// ROUTER_CFG_BINDING in vrt-config), and a cross-language test
// asserts this file's literals stay identical — a user-declared
// assets binding name (e.g. `"STATIC"`) must never leak into the
// router's own env keys.

/// Fixed env binding name the router uses for the assets facade —
/// decoupled from the user's declared binding name.
export const ASSETS_BINDING = "ASSETS";
/// Fixed env binding name for the user worker fallback.
export const USER_WORKER_BINDING = "USER_WORKER";
/// Fixed env binding name for the router policy JSON.
export const ROUTER_CFG_BINDING = "VRT_ROUTER_CFG";

// ── route-index input ────────────────────────────────────────────────
// `fetchRouteIndex` takes no request data — the index is a property of
// the manifest (the binding's authorized resource), not of the request.
// A fixed, validatable marker input keeps the RPC signature explicit.
export interface RouteIndexRequest {
  kind: "route-index";
}

export const ROUTE_INDEX_REQUEST: RouteIndexRequest = Object.freeze({
  kind: "route-index",
});

export function isRouteIndexRequest(v: unknown): v is RouteIndexRequest {
  return (
    typeof v === "object" &&
    v !== null &&
    (v as { kind?: unknown }).kind === "route-index"
  );
}

// ── asset request wire ───────────────────────────────────────────────
// The request-bearing shape used by `fetchAsset`, `canFetchAsset` and
// the facade's internal `#backendCall` encoding. `headers` stays a
// wire pair list (never a Headers instance — RPC boundaries pass
// structured-cloneable values only).
export interface AssetRequestWire {
  url?: unknown;
  method?: unknown;
  headers?: unknown;
  hasStaticRouting?: unknown;
}

// ── can-fetch probe input ────────────────────────────────────────────
// The subset of a request the provider's not-found gating needs.
export interface CanFetchInput extends AssetRequestWire {
  url: string;
  method: string;
  headers: [string, string][];
  hasStaticRouting: boolean;
}

export function canFetchInput(
  request: Request,
  hasStaticRouting: boolean,
): CanFetchInput {
  return {
    url: request.url,
    method: request.method,
    headers: [...request.headers] as [string, string][],
    hasStaticRouting,
  };
}

// ── bounded route-failure reasons ────────────────────────────────────
// Emitted on the 503 `x-vrt-route-error` header; ingress maps these to
// a bounded `reason` label on vrt_worker_assets_route_requests_total.
// Keep this a small closed set — the host whitelist rejects others.
export const ROUTE_ERROR_REASONS = [
  "route_index_rpc_failed",
  "route_index_invalid",
  "route_index_digest_mismatch",
  "route_index_timeout",
  "can_fetch_rpc_failed",
  "can_fetch_timeout",
] as const;
export type RouteErrorReason = (typeof ROUTE_ERROR_REASONS)[number];

export const ROUTE_ERROR_HEADER = "x-vrt-route-error";
