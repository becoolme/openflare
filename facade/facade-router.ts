// facade-router system worker — Miniflare-compatible assets-first
// routing. Sits in front of the user worker for tenants whose
// wrangler/vrt.toml opts into `assets_first`: route-index hit → the
// facade-assets binding; miss → the user worker. Pure routing logic
// lives in `router-core.ts`; the wire contract with facade-assets in
// `router-wire.ts` (T31f/F0 — both sides import the same types so the
// contract cannot silently fork).
//
// Entry discipline (T25/L12 + T31e/E0): this module exports ONLY the
// entrypoint class and its `export default`. Anything else (test
// probes, helpers) lives in router-core.ts — a plain default class
// export is treated by workerd as an actor class and every request
// silently falls through to globalOutbound.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  compileCfg,
  cacheGet,
  cachePut,
  fetchRouteIndexDedup,
  normalizePath,
  intentFetchable,
  globMatch,
  withTimeout,
  RpcTimeoutError,
  RPC_BUDGET,
  type RouteIndex,
} from "./router-core.js";
import {
  ROUTE_INDEX_REQUEST,
  canFetchInput,
  type RouteErrorReason,
  ROUTE_ERROR_HEADER,
} from "./router-wire.js";

const ASSET_ROUTE_HEADER = "x-vrt-asset-route";

/// F0.4: every silent branch emits a structured diagnostic — the
/// fallback must be observable (AGENTS.md: fallbacks must be observable, never silently
/// swallow errors). Log shape is fixed: code + digest prefix + error name/code;
/// never props, tokens, or request headers.
function routeError(
  code: RouteErrorReason,
  digest: string,
  err: unknown,
): void {
  const name = err instanceof Error ? err.name : typeof err;
  const detail =
    err instanceof Error &&
    "code" in err &&
    typeof (err as { code?: unknown }).code === "string"
      ? (err as { code: string }).code
      : "";
  console.error(
    JSON.stringify({
      code: code.toUpperCase(),
      digestPrefix: digest.slice(0, 12),
      error: name,
      detail,
    }),
  );
}

interface RouterEnv {
  USER_WORKER: { fetch(req: Request): Promise<Response> };
  ASSETS: {
    fetch(req: Request): Promise<Response>;
    fetchRouteIndex(input: unknown): Promise<unknown>;
    canFetchAsset(input: unknown): Promise<boolean>;
  };
  VRT_ROUTER_CFG: string;
}

function stripMarker(response: Response): Response {
  const hasMarker =
    response.headers.has(ASSET_ROUTE_HEADER) ||
    response.headers.has(ROUTE_ERROR_HEADER);
  if (!hasMarker) return response;
  const headers = new Headers(response.headers);
  headers.delete(ASSET_ROUTE_HEADER);
  // A user worker must never forge the route-failure reason — the
  // host trusts it for the assets-route `reason` metric label.
  headers.delete(ROUTE_ERROR_HEADER);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function marked(response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set(ASSET_ROUTE_HEADER, "assets");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

const ROUTE_UNREACHABLE = (reason: RouteErrorReason): Response => {
  const headers = new Headers({
    "content-type": "application/json",
    [ASSET_ROUTE_HEADER]: "assets",
    // Bounded enum the host maps onto the `reason` label of
    // vrt_worker_assets_route_requests_total — scrubbed before the
    // response ever leaves the pod.
    [ROUTE_ERROR_HEADER]: reason,
  });
  return new Response(
    JSON.stringify({ error: { code: "ASSETS_ROUTE_UNAVAILABLE" } }),
    { status: 503, headers },
  );
};

export class AssetsRouter extends WorkerEntrypoint<RouterEnv> {
  async fetch(request: Request): Promise<Response> {
    const env = this.env;
    const cfg = compileCfg(env.VRT_ROUTER_CFG ?? "");
    if (cfg === undefined) {
      // Invalid platform cfg — same fail-closed 503, but still a
      // logged signal (no digest bound yet: log the raw-text length,
      // never the cfg body itself).
      routeError(
        "route_index_invalid",
        "",
        new Error(
          `VRT_ROUTER_CFG failed compile (len=${env.VRT_ROUTER_CFG?.length ?? 0})`,
        ),
      );
      return ROUTE_UNREACHABLE("route_index_invalid");
    }
    const hasStaticRouting =
      cfg.userWorker.length + cfg.assetWorker.length > 0;
    const toUserWorker = async () => stripMarker(await env.USER_WORKER.fetch(request));
    const toAssets = async () => marked(await env.ASSETS.fetch(request));

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

    const fetchable = await this.#fetchable(
      request,
      pathname,
      rawPath,
      navigate,
      hasStaticRouting,
      cfg.manifestDigest,
    );
    if (fetchable.error) return ROUTE_UNREACHABLE(fetchable.error);
    return fetchable.value ? toAssets() : toUserWorker();
  }

  /// `undefined`-free result: every failure path carries the bounded
  /// reason the host counts under the assets-route `reason` label.
  async #fetchable(
    request: Request,
    pathname: string,
    rawPath: string,
    navigate: boolean,
    hasStaticRouting: boolean,
    digest: string,
  ): Promise<{ value: boolean; error?: RouteErrorReason }> {
    const env = this.env;
    let slot = cacheGet(digest);
    if (slot === undefined) {
      let parsed;
      try {
        // Dedup: N concurrent cold-cache requests share one RPC+parse
        // (T31f/F2); the input is the fixed ROUTE_INDEX_REQUEST marker
        // — the index is a property of the manifest, not the request.
        // T31g/G1: bounded by RPC_BUDGET — a hung backend must not
        // stall the request indefinitely.
        parsed = await withTimeout(
          fetchRouteIndexDedup(digest, () =>
            env.ASSETS.fetchRouteIndex(ROUTE_INDEX_REQUEST),
          ),
          RPC_BUDGET.indexMs,
          "route-index",
        );
      } catch (err) {
        const reason: RouteErrorReason =
          err instanceof RpcTimeoutError
            ? "route_index_timeout"
            : "route_index_rpc_failed";
        routeError(reason, digest, err);
        return { value: false, error: reason };
      }
      if (parsed.kind === "invalid") {
        routeError(
          "route_index_invalid",
          digest,
          new Error("route-index response failed wire validation"),
        );
        return { value: false, error: "route_index_invalid" };
      }
      if (parsed.kind === "tooLarge") {
        // Over the cacheable bound — degrade to the single-shot probe,
        // counted by the provider as outcome `probe`. Not a failure.
        // Cache the marker so later requests skip the index fetch
        // entirely (T31d/E2).
        cachePut({ kind: "tooLarge", digest, approxBytes: 8 });
        return this.#probe(request, hasStaticRouting, digest);
      }
      // E3: the index must declare the manifest it describes — the
      // digest must match the cfg binding or the index is unusable.
      if (parsed.index.manifestDigest !== digest) {
        routeError(
          "route_index_digest_mismatch",
          digest,
          new Error(
            `index declared ${parsed.index.manifestDigest.slice(0, 12)}`,
          ),
        );
        return { value: false, error: "route_index_digest_mismatch" };
      }
      const index: RouteIndex = parsed.index;
      index.digest = digest;
      slot = { kind: "index", index };
      cachePut(slot);
    }
    if (slot.kind === "tooLarge") {
      return this.#probe(request, hasStaticRouting, digest);
    }
    if (slot.kind !== "index") {
      routeError(
        "route_index_invalid",
        digest,
        new Error("unreachable cache slot kind"),
      );
      return { value: false, error: "route_index_invalid" };
    }
    return {
      value: intentFetchable(
        slot.index,
        pathname,
        navigate,
        hasStaticRouting,
        rawPath,
      ),
    };
  }

  async #probe(
    request: Request,
    hasStaticRouting: boolean,
    digest: string,
  ): Promise<{ value: boolean; error?: RouteErrorReason }> {
    try {
      return {
        value: await withTimeout(
          this.env.ASSETS.canFetchAsset(
            canFetchInput(request, hasStaticRouting),
          ),
          RPC_BUDGET.probeMs,
          "can-fetch",
        ),
      };
    } catch (err) {
      const reason: RouteErrorReason =
        err instanceof RpcTimeoutError
          ? "can_fetch_timeout"
          : "can_fetch_rpc_failed";
      routeError(reason, digest, err);
      return { value: false, error: reason };
    }
  }
}

export default AssetsRouter;
