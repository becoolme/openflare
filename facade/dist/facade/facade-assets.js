import { WorkerEntrypoint } from "cloudflare:workers";
import { isRouteIndexRequest } from "./router-wire.js";
import { INTERNAL_HEADERS, bindingError } from "../third_party/open-compute/facade/loader/shared.js";
import { identityHeaders, resourceProps } from "./props.js";
const ASSET_PATH = "http://binding-backend/internal/assets/v1/fetch";
const ROUTE_INDEX_PATH = "http://binding-backend/internal/assets/v1/route-index";
const CAN_FETCH_PATH = "http://binding-backend/internal/assets/v1/can-fetch";
const MAX_ASSET_HEADERS = 256;
const ALLOWED_METHODS = new Set([
    "GET",
    "HEAD"
]);
const ALLOWED_ERROR_CODES = new Set([
    "BINDING_PROTOCOL_ERROR",
    "BINDING_LIMIT_EXCEEDED",
    "BINDING_PERMISSION_DENIED",
    "BINDING_CAPABILITY_UNSUPPORTED",
    "BINDING_RESOURCE_GONE",
    "ASSET_STORAGE_UNAVAILABLE",
    "ASSET_NOT_FOUND",
    "ASSET_TOO_LARGE",
    "ASSET_INTERNAL_ERROR"
]);
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
    "location"
]);
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
    "host"
]);
const CUSTOM_HEADER_NAME_RE = new RegExp("^[a-z0-9!#$%&'*+.^_\\x60|~-]+$");
function isForwardableCustomHeader(name) {
    return CUSTOM_HEADER_NAME_RE.test(name) && !name.startsWith("x-open-compute-") && !name.startsWith("x-vrt-") && !CUSTOM_HEADER_DENY.has(name);
}
function sanitizeErrorCode(value) {
    return value && ALLOWED_ERROR_CODES.has(value) ? value : "ASSET_STORAGE_UNAVAILABLE";
}
function isRecord(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function assetRequest(input) {
    if (!isRecord(input) || typeof input.url !== "string" || typeof input.method !== "string" || !Array.isArray(input.headers) || input.headers.length > MAX_ASSET_HEADERS) {
        throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    const headers = new Headers();
    for (const pair of input.headers){
        if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== "string" || typeof pair[1] !== "string") {
            throw bindingError("BINDING_PROTOCOL_ERROR");
        }
        headers.append(pair[0], pair[1]);
    }
    try {
        return new Request(input.url, {
            method: input.method,
            headers
        });
    } catch  {
        throw bindingError("BINDING_PROTOCOL_ERROR");
    }
}
export class AssetTransport extends WorkerEntrypoint {
    #props;
    constructor(ctx, env){
        super(ctx, env);
        this.#props = resourceProps(ctx?.props);
    }
    async fetchAsset(input) {
        return this.#fetch(assetRequest(input));
    }
    fetch(request) {
        return this.#fetch(request);
    }
    async #fetch(request) {
        const method = request.method.toUpperCase();
        if (!ALLOWED_METHODS.has(method)) {
            throw bindingError("BINDING_CAPABILITY_UNSUPPORTED");
        }
        const backend = this.env?.BINDING_BACKEND;
        if (!backend || typeof backend.fetch !== "function") {
            throw bindingError("BINDING_PROTOCOL_ERROR");
        }
        const headers = new Headers(request.headers);
        for (const name of INTERNAL_HEADERS)headers.delete(name);
        for (const name of [
            ...headers.keys()
        ]){
            if (name.toLowerCase().startsWith("x-open-compute-")) headers.delete(name);
        }
        for (const [key, value] of Object.entries(identityHeaders(this.#props))){
            headers.set(key, value);
        }
        headers.set("x-open-compute-request-id", crypto.randomUUID());
        headers.set("x-open-compute-asset-method", method);
        headers.set("x-open-compute-asset-url", request.url);
        const response = await backend.fetch(ASSET_PATH, {
            method: "POST",
            headers,
            redirect: "manual"
        });
        return this.#finish(response, method);
    }
    async #backendCall(path, input) {
        const backend = this.env?.BINDING_BACKEND;
        if (!backend || typeof backend.fetch !== "function") {
            throw bindingError("BINDING_PROTOCOL_ERROR");
        }
        const headers = new Headers();
        for (const [key, value] of Object.entries(identityHeaders(this.#props))){
            headers.set(key, value);
        }
        headers.set("x-open-compute-request-id", crypto.randomUUID());
        headers.set("x-open-compute-asset-method", typeof input.method === "string" ? input.method.toUpperCase() : "GET");
        headers.set("x-open-compute-asset-url", typeof input.url === "string" ? input.url : "http://localhost/");
        if (input.hasStaticRouting === true) {
            headers.set("x-open-compute-asset-has-static-routing", "true");
        }
        if (Array.isArray(input.headers)) {
            for (const pair of input.headers.slice(0, MAX_ASSET_HEADERS)){
                if (Array.isArray(pair) && pair.length === 2 && typeof pair[0] === "string" && typeof pair[1] === "string" && pair[0].toLowerCase() === "sec-fetch-mode") {
                    headers.set("sec-fetch-mode", pair[1]);
                }
            }
        }
        return backend.fetch(path, {
            method: "POST",
            headers,
            redirect: "manual"
        });
    }
    async fetchRouteIndex(input) {
        if (!isRouteIndexRequest(input)) {
            throw bindingError("BINDING_PROTOCOL_ERROR");
        }
        const response = await this.#backendCall(ROUTE_INDEX_PATH, {
            url: ROUTE_INDEX_PATH,
            method: "POST",
            headers: []
        });
        if (!response.ok && response.headers.has("x-open-compute-error-code")) {
            throw bindingError(sanitizeErrorCode(response.headers.get("x-open-compute-error-code")));
        }
        if (response.status !== 200) {
            throw bindingError("ASSET_STORAGE_UNAVAILABLE");
        }
        return response.json();
    }
    async canFetchAsset(input) {
        const wire = input;
        const request = assetRequest(wire);
        const response = await this.#backendCall(CAN_FETCH_PATH, {
            url: request.url,
            method: request.method,
            headers: wire?.headers,
            hasStaticRouting: wire?.hasStaticRouting
        });
        if (!response.ok && response.headers.has("x-open-compute-error-code")) {
            throw bindingError(sanitizeErrorCode(response.headers.get("x-open-compute-error-code")));
        }
        if (response.status !== 200) {
            throw bindingError("ASSET_STORAGE_UNAVAILABLE");
        }
        const text = (await response.text()).trim();
        return text === "true";
    }
    #finish(response, method) {
        if (response.status === 404 && response.headers.get("x-open-compute-error-code") === "ASSET_NOT_FOUND") {
            return new Response(method === "HEAD" ? null : "Not Found", {
                status: 404,
                headers: method === "HEAD" ? undefined : {
                    "content-type": "text/plain;charset=UTF-8"
                }
            });
        }
        if (!response.ok && response.headers.has("x-open-compute-error-code")) {
            throw bindingError(sanitizeErrorCode(response.headers.get("x-open-compute-error-code")));
        }
        const responseHeaders = new Headers();
        for (const name of PASSTHROUGH_RESPONSE_HEADERS){
            const value = response.headers.get(name);
            if (value !== null) responseHeaders.set(name, value);
        }
        const customNames = response.headers.get(CUSTOM_HEADER_NAMES);
        if (customNames !== null) {
            for (const raw of customNames.split(",").slice(0, MAX_CUSTOM_HEADER_NAMES)){
                const name = raw.trim().toLowerCase();
                if (!isForwardableCustomHeader(name)) continue;
                if (name === "set-cookie") {
                    const getSetCookie = response.headers.getSetCookie;
                    const cookies = typeof getSetCookie === "function" ? getSetCookie.call(response.headers) : [];
                    if (cookies.length > 0) {
                        for (const cookie of cookies)responseHeaders.append("set-cookie", cookie);
                        continue;
                    }
                }
                const value = response.headers.get(name);
                if (value !== null) responseHeaders.set(name, value);
            }
        }
        const representationLength = response.headers.get("x-open-compute-asset-representation-length") ?? responseHeaders.get("content-length");
        if (representationLength) {
            responseHeaders.set("x-open-compute-asset-representation-length", representationLength);
        }
        const forwarded = new Response(response.body, {
            status: response.status,
            statusText: response.statusText,
            headers: responseHeaders
        });
        if (representationLength) {
            forwarded.headers.set("content-length", representationLength);
        }
        return forwarded;
    }
}
export default AssetTransport;
