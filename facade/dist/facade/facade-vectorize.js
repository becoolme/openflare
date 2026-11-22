import { WorkerEntrypoint } from "cloudflare:workers";
import { resourceProps, wrapBindingBackend } from "./props.js";
import { VECTORIZE_PROVIDER_PREFIX, VECTORIZE_POST_OPS, VECTORIZE_GET_OPS, VECTORIZE_STRIP_HEADER_PREFIX, OPEN_COMPUTE_PREFIX, VECTORIZE_BODY_MAX_BYTES, VECTORIZE_DRAIN_MAX_BYTES, VECTORIZE_BACKEND_TIMEOUT_MS } from "./vectorize-wire.js";
import { drainToBuffer } from "./queue-wire.js";
import { BACKEND_TIMEOUT, backendCallWithTimeout } from "./deadline.js";
function vecError(code, error, status) {
    return new Response(JSON.stringify({
        code,
        error
    }), {
        status,
        headers: {
            "content-type": "application/json"
        }
    });
}
export class VectorizeFacade extends WorkerEntrypoint {
    async fetch(request) {
        try {
            const props = resourceProps(this.ctx.props);
            const backend = wrapBindingBackend(this.env.BINDING_BACKEND, this.ctx.props);
            const url = new URL(request.url);
            const leaf = url.pathname.split("/").filter(Boolean).pop() ?? "";
            const isGet = VECTORIZE_GET_OPS.includes(leaf);
            const isPost = VECTORIZE_POST_OPS.includes(leaf);
            if (!isGet && !isPost || isGet && request.method !== "GET" || isPost && request.method !== "POST") {
                return vecError(3000, "unsupported vectorize route", 404);
            }
            let body = null;
            if (isPost) {
                const drained = await drainToBuffer(request.body, VECTORIZE_BODY_MAX_BYTES, VECTORIZE_DRAIN_MAX_BYTES);
                if (!drained.ok) {
                    return vecError(3001, "request body too large", 413);
                }
                body = drained.body;
            }
            const headers = new Headers(request.headers);
            for (const key of [
                ...headers.keys()
            ]){
                const k = key.toLowerCase();
                if (k.startsWith(VECTORIZE_STRIP_HEADER_PREFIX) || k.startsWith(OPEN_COMPUTE_PREFIX)) {
                    headers.delete(key);
                }
            }
            headers.set("x-open-compute-request-id", crypto.randomUUID());
            const target = new URL(`${VECTORIZE_PROVIDER_PREFIX}${props.bindingId}/${leaf}${url.search}`, request.url).href;
            const resp = await backendCallWithTimeout(backend, target, {
                method: request.method,
                headers,
                body,
                redirect: "manual"
            }, VECTORIZE_BACKEND_TIMEOUT_MS);
            if (resp === BACKEND_TIMEOUT) {
                return vecError(3059, "vectorize backend timeout", 504);
            }
            if (resp instanceof Error) {
                return vecError(3058, "vectorize backend error", 502);
            }
            return resp;
        } catch  {
            return vecError(3057, "vectorize facade error", 500);
        }
    }
}
export default VectorizeFacade;
