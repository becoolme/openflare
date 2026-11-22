import { WorkerEntrypoint } from "cloudflare:workers";
import { resourceProps, wrapBindingBackend } from "./props.js";
import { K2_PROVIDER_PREFIX, K2_OPS, K2_STRIP_HEADER_PREFIX, OPEN_COMPUTE_PREFIX, K2_BODY_MAX_BYTES, K2_DRAIN_MAX_BYTES, K2_BACKEND_TIMEOUT_MS } from "./k2-wire.js";
import { drainToBuffer } from "./queue-wire.js";
import { BACKEND_TIMEOUT, backendCallWithTimeout } from "./deadline.js";
function k2Error(code, message, status = 400) {
    return new Response(JSON.stringify({
        error: {
            code,
            message,
            retryable: false
        }
    }), {
        status,
        headers: {
            "content-type": "application/json"
        }
    });
}
export class K2Facade extends WorkerEntrypoint {
    async fetch(request) {
        try {
            const props = resourceProps(this.ctx.props);
            const backend = wrapBindingBackend(this.env.BINDING_BACKEND, this.ctx.props);
            const url = new URL(request.url);
            const drained = await drainToBuffer(request.body, K2_BODY_MAX_BYTES, K2_DRAIN_MAX_BYTES);
            if (!drained.ok) {
                return k2Error("BATCH_TOO_LARGE", "k2 request body too large", 413);
            }
            const leaf = url.pathname.split("/").filter(Boolean).pop() ?? "";
            const op = K2_OPS.includes(leaf) ? leaf : null;
            if (op === null || request.method !== "POST") {
                return k2Error("BAD_ROUTE", "unsupported k2 route", 404);
            }
            const headers = new Headers(request.headers);
            for (const key of [
                ...headers.keys()
            ]){
                const k = key.toLowerCase();
                if (k.startsWith(K2_STRIP_HEADER_PREFIX) || k.startsWith(OPEN_COMPUTE_PREFIX)) {
                    headers.delete(key);
                }
            }
            headers.set("x-open-compute-request-id", crypto.randomUUID());
            const target = new URL(`${K2_PROVIDER_PREFIX}${props.bindingId}/${op}${url.search}`, request.url).href;
            const resp = await backendCallWithTimeout(backend, target, {
                method: "POST",
                headers,
                body: drained.body,
                redirect: "manual"
            }, K2_BACKEND_TIMEOUT_MS);
            if (resp === BACKEND_TIMEOUT) {
                return k2Error("BACKEND_TIMEOUT", "k2 backend timeout", 504);
            }
            if (resp instanceof Error) {
                return k2Error("BACKEND_ERROR", "k2 backend error", 502);
            }
            return resp;
        } catch  {
            return k2Error("FACADE_ERROR", "facade error", 500);
        }
    }
}
export default K2Facade;
