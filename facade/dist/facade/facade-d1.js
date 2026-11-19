import { WorkerEntrypoint } from "cloudflare:workers";
import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";
import { resourceProps, wrapBindingBackend } from "./props.js";
import { BACKEND_TIMEOUT, D1_BACKEND_TIMEOUT_MS, backendCallWithTimeout } from "./deadline.js";
import { D1_PROVIDER_PREFIX, D1_OPS, D1_STRIP_HEADER_PREFIX, OPEN_COMPUTE_PREFIX } from "./d1-wire.js";
function d1Error(message, status = 200) {
    return new Response(JSON.stringify({
        success: false,
        error: `D1_ERROR: ${message}`
    }), {
        status,
        headers: {
            "content-type": "application/json"
        }
    });
}
export class D1Facade extends WorkerEntrypoint {
    async fetch(request) {
        try {
            const props = resourceProps(this.ctx.props);
            const backend = wrapBindingBackend(this.env.BINDING_BACKEND, this.ctx.props);
            const url = new URL(request.url);
            const leaf = url.pathname.split("/").filter(Boolean).pop() ?? "";
            const op = D1_OPS.includes(leaf) ? leaf : null;
            if (request.method !== "POST" || op === null) {
                return d1Error("unsupported d1 route", 404);
            }
            const headers = new Headers(request.headers);
            for (const key of [
                ...headers.keys()
            ]){
                const k = key.toLowerCase();
                if (k.startsWith(D1_STRIP_HEADER_PREFIX) || k.startsWith(OPEN_COMPUTE_PREFIX)) {
                    headers.delete(key);
                }
            }
            headers.set("x-open-compute-request-id", crypto.randomUUID());
            const target = new URL(`${D1_PROVIDER_PREFIX}${props.bindingId}/${op}${url.search}`, request.url).href;
            const resp = await backendCallWithTimeout(backend, target, {
                method: "POST",
                headers,
                body: request.body,
                redirect: "manual"
            }, D1_BACKEND_TIMEOUT_MS);
            if (resp === BACKEND_TIMEOUT) {
                return d1Error("backend timeout (504) - retryable");
            }
            if (resp instanceof Error) {
                return d1Error("backend error (502) - retryable");
            }
            if (!(resp instanceof Response)) {
                return d1Error("backend protocol error");
            }
            if (!resp.ok) {
                return d1Error(`backend denied request (${resp.status})`);
            }
            return resp;
        } catch  {
            return d1Error("facade error");
        }
    }
}
export default D1Facade;
