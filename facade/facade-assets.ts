// facade-assets system worker entry: minimal AssetTransport (vrt-owned, not
// vendored). Reads version-scoped static assets from binding-backend;
// GET/HEAD only in Phase 1 — any other method is an explicit
// BINDING_CAPABILITY_UNSUPPORTED, never a silent pass.
// Props: ResourceBindingProps with namespaceResourceId = manifestDigest.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  isRouteIndexRequest,
  type AssetRequestWire,
} from "./router-wire.js";
import {
  INTERNAL_HEADERS,
  bindingError,
} from "../third_party/open-compute/facade/loader/shared.js";
import {
  identityHeaders,
  resourceProps,
  type ResourceProps,
} from "./props.js";

const ASSET_PATH = "http://binding-backend/internal/assets/v1/fetch";
// T31c: router probe endpoints — same BindingContext authorization
// path, same identity minting. `route-index` returns the bounded
// manifest index the router caches; `can-fetch` is the degradation
// path for manifests too large to index.
const ROUTE_INDEX_PATH = "http://binding-backend/internal/assets/v1/route-index";
const CAN_FETCH_PATH = "http://binding-backend/internal/assets/v1/can-fetch";
const MAX_ASSET_HEADERS = 256;
const ALLOWED_METHODS = new Set(["GET", "HEAD"]);

// T13/L2: backend error codes passed to bindingError are whitelisted —
// a backend must never smuggle an arbitrary code/string into the
// worker-visible error surface.
const ALLOWED_ERROR_CODES = new Set([
  "BINDING_PROTOCOL_ERROR",
  "BINDING_LIMIT_EXCEEDED",
  "BINDING_PERMISSION_DENIED",
  "BINDING_CAPABILITY_UNSUPPORTED",
  "BINDING_RESOURCE_GONE",
  "ASSET_STORAGE_UNAVAILABLE",
  "ASSET_NOT_FOUND",
  "ASSET_TOO_LARGE",
  "ASSET_INTERNAL_ERROR",
]);

// T13/L2: response header whitelist — internal x-open-compute-* fields
// never reach the worker; only representation headers pass through.
const PASSTHROUGH_RESPONSE_HEADERS = new Set([
  "content-type",
  "content-length",
  "content-range",
  "content-encoding",
  "content-language",
  "cache-control",
  "etag",
  "last-modified",
  "expires",
  "vary",
  // T21: trailing-slash redirects (auto-trailing-slash) are expressed
  // as 307/308 with a location target — location is a representation
  // header the worker must see for the redirect to be followable.
  "location",
]);

/// Internal list of `_headers`-touched names (provider → facade). Only
/// these names are forwarded in addition to the representation
/// whitelist; anything else the backend emits stays internal.
const CUSTOM_HEADER_NAMES = "x-open-compute-asset-header-names";
const MAX_CUSTOM_HEADER_NAMES = 256;
const CUSTOM_HEADER_DENY = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "content-length",
  "host",
]);
// Built via RegExp so the source contains no backtick (the manifest
// generator strips comments with a backtick-unaware scanner).
const CUSTOM_HEADER_NAME_RE = new RegExp("^[a-z0-9!#$%&'*+.^_\\x60|~-]+$");

function isForwardableCustomHeader(name: string): boolean {
  return (
    CUSTOM_HEADER_NAME_RE.test(name) &&
    !name.startsWith("x-open-compute-") &&
    !name.startsWith("x-vrt-") &&
    !CUSTOM_HEADER_DENY.has(name)
  );
}

function sanitizeErrorCode(value: string | null): string {
  return value && ALLOWED_ERROR_CODES.has(value)
    ? value
    : "ASSET_STORAGE_UNAVAILABLE";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function assetRequest(input: AssetRequestWire): Request {
  if (
    !isRecord(input) ||
    typeof input.url !== "string" ||
    typeof input.method !== "string" ||
    !Array.isArray(input.headers) ||
    input.headers.length > MAX_ASSET_HEADERS
  ) {
    throw bindingError("BINDING_PROTOCOL_ERROR");
  }
  const headers = new Headers();
  for (const pair of input.headers) {
    if (
      !Array.isArray(pair) ||
      pair.length !== 2 ||
      typeof pair[0] !== "string" ||
      typeof pair[1] !== "string"
    ) {
      throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    headers.append(pair[0], pair[1]);
  }
  try {
    return new Request(input.url, { method: input.method, headers });
  } catch {
    throw bindingError("BINDING_PROTOCOL_ERROR");
  }
}

export class AssetTransport extends WorkerEntrypoint {
  readonly #props: ResourceProps;

  constructor(ctx: { props?: unknown }, env: unknown) {
    super(ctx, env);
    this.#props = resourceProps(ctx?.props);
  }

  async fetchAsset(input: unknown): Promise<Response> {
    return this.#fetch(assetRequest(input as AssetRequestWire));
  }

  fetch(request: Request): Promise<Response> {
    return this.#fetch(request);
  }

  async #fetch(request: Request): Promise<Response> {
    const method = request.method.toUpperCase();
    if (!ALLOWED_METHODS.has(method)) {
      throw bindingError("BINDING_CAPABILITY_UNSUPPORTED");
    }
    const backend = (this.env as Record<string, unknown> | undefined)
      ?.BINDING_BACKEND as { fetch?: unknown } | undefined;
    if (!backend || typeof backend.fetch !== "function") {
      throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    const headers = new Headers(request.headers);
    for (const name of INTERNAL_HEADERS) headers.delete(name);
    // INTERNAL_HEADERS is an upstream-maintained named list; as a safety net also strip any
    // x-open-compute-* prefixed header, so the identity surface is fully overwritten by props.
    for (const name of [...headers.keys()]) {
      if (name.toLowerCase().startsWith("x-open-compute-")) headers.delete(name);
    }
    for (const [key, value] of Object.entries(identityHeaders(this.#props))) {
      headers.set(key, value);
    }
    headers.set("x-open-compute-request-id", crypto.randomUUID());
    headers.set("x-open-compute-asset-method", method);
    headers.set("x-open-compute-asset-url", request.url);
    const response = await (backend as {
      fetch(input: string, init: RequestInit): Promise<Response>;
    }).fetch(ASSET_PATH, { method: "POST", headers, redirect: "manual" });
    return this.#finish(response, method);
  }

  // Shared binding-backend call for the T31c router endpoints —
  // same identity minting, same header stripping; the only difference
  // is the path and the small JSON response contract.
  async #backendCall(
    path: string,
    input: AssetRequestWire,
  ): Promise<Response> {
    const backend = (this.env as Record<string, unknown> | undefined)
      ?.BINDING_BACKEND as { fetch?: unknown } | undefined;
    if (!backend || typeof backend.fetch !== "function") {
      throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    const headers = new Headers();
    for (const [key, value] of Object.entries(identityHeaders(this.#props))) {
      headers.set(key, value);
    }
    headers.set("x-open-compute-request-id", crypto.randomUUID());
    headers.set(
      "x-open-compute-asset-method",
      typeof input.method === "string" ? input.method.toUpperCase() : "GET",
    );
    headers.set(
      "x-open-compute-asset-url",
      typeof input.url === "string" ? input.url : "http://localhost/",
    );
    if (input.hasStaticRouting === true) {
      headers.set("x-open-compute-asset-has-static-routing", "true");
    }
    // Forward the navigation signal the not-found gate keys on.
    if (Array.isArray(input.headers)) {
      for (const pair of input.headers.slice(0, MAX_ASSET_HEADERS)) {
        if (
          Array.isArray(pair) &&
          pair.length === 2 &&
          typeof pair[0] === "string" &&
          typeof pair[1] === "string" &&
          pair[0].toLowerCase() === "sec-fetch-mode"
        ) {
          headers.set("sec-fetch-mode", pair[1]);
        }
      }
    }
    return (backend as {
      fetch(input: string, init: RequestInit): Promise<Response>;
    }).fetch(path, { method: "POST", headers, redirect: "manual" });
  }

  /// T31c: `POST /internal/assets/v1/route-index` — bounded manifest
  /// index for the router's in-isolate cache. Returns the parsed
  /// `{htmlHandling, notFoundHandling, manifestDigest, paths}` document.
  /// The input is a fixed `ROUTE_INDEX_REQUEST` marker (router-wire.ts)
  /// — the index is a property of the manifest, not of the request.
  async fetchRouteIndex(input: unknown): Promise<unknown> {
    if (!isRouteIndexRequest(input)) {
      throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    const response = await this.#backendCall(ROUTE_INDEX_PATH, {
      url: ROUTE_INDEX_PATH,
      method: "POST",
      headers: [],
    });
    if (!response.ok && response.headers.has("x-open-compute-error-code")) {
      throw bindingError(
        sanitizeErrorCode(response.headers.get("x-open-compute-error-code")),
      );
    }
    if (response.status !== 200) {
      throw bindingError("ASSET_STORAGE_UNAVAILABLE");
    }
    return response.json();
  }

  /// T31c: `POST /internal/assets/v1/can-fetch` — degradation probe
  /// used when the route index cannot be cached.
  async canFetchAsset(input: unknown): Promise<boolean> {
    const wire = input as AssetRequestWire;
    const request = assetRequest(wire);
    const response = await this.#backendCall(CAN_FETCH_PATH, {
      url: request.url,
      method: request.method,
      headers: wire?.headers,
      hasStaticRouting: wire?.hasStaticRouting,
    });
    if (!response.ok && response.headers.has("x-open-compute-error-code")) {
      throw bindingError(
        sanitizeErrorCode(response.headers.get("x-open-compute-error-code")),
      );
    }
    if (response.status !== 200) {
      throw bindingError("ASSET_STORAGE_UNAVAILABLE");
    }
    const text = (await response.text()).trim();
    return text === "true";
  }

  #finish(response: Response, method: string): Response {
    // Cloudflare compat: ASSETS.fetch() never throws for a missing asset —
    // the typed ASSET_NOT_FOUND 404 becomes a plain 404 Response (no
    // internal headers, no body for HEAD). Every other error code throws.
    if (
      response.status === 404 &&
      response.headers.get("x-open-compute-error-code") === "ASSET_NOT_FOUND"
    ) {
      return new Response(method === "HEAD" ? null : "Not Found", {
        status: 404,
        headers:
          method === "HEAD" ? undefined : { "content-type": "text/plain;charset=UTF-8" },
      });
    }
    if (!response.ok && response.headers.has("x-open-compute-error-code")) {
      throw bindingError(
        sanitizeErrorCode(response.headers.get("x-open-compute-error-code")),
      );
    }
    // T13/L2: strip internal x-open-compute-* headers — forward only
    // whitelisted representation headers to the worker.
    const responseHeaders = new Headers();
    for (const name of PASSTHROUGH_RESPONSE_HEADERS) {
      const value = response.headers.get(name);
      if (value !== null) responseHeaders.set(name, value);
    }
    // `_headers` output: the provider enumerates the names it touched
    // (set or detached); forward exactly those, re-validated here so a
    // compromised backend still cannot inject internal headers.
    const customNames = response.headers.get(CUSTOM_HEADER_NAMES);
    if (customNames !== null) {
      for (const raw of customNames.split(",").slice(0, MAX_CUSTOM_HEADER_NAMES)) {
        const name = raw.trim().toLowerCase();
        if (!isForwardableCustomHeader(name)) continue;
        if (name === "set-cookie") {
          const getSetCookie = (
            response.headers as { getSetCookie?: () => string[] }
          ).getSetCookie;
          const cookies =
            typeof getSetCookie === "function"
              ? getSetCookie.call(response.headers)
              : [];
          if (cookies.length > 0) {
            for (const cookie of cookies) responseHeaders.append("set-cookie", cookie);
            continue;
          }
        }
        const value = response.headers.get(name);
        if (value !== null) responseHeaders.set(name, value);
      }
    }
    // Internal representation-length is read from the BACKEND response
    // and re-emitted explicitly — it is not a passthrough header.
    const representationLength =
      response.headers.get("x-open-compute-asset-representation-length") ??
      responseHeaders.get("content-length");
    if (representationLength) {
      responseHeaders.set(
        "x-open-compute-asset-representation-length",
        representationLength,
      );
    }
    const forwarded = new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders,
    });
    if (representationLength) {
      forwarded.headers.set("content-length", representationLength);
    }
    return forwarded;
  }
}
export default AssetTransport;
