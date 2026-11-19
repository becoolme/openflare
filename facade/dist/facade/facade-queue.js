import { WorkerEntrypoint } from "cloudflare:workers";
import { resourceProps, wrapBindingBackend } from "./props.js";
import { QUEUE_PROVIDER_PREFIX, QUEUE_OPS, QUEUE_STRIP_HEADER_PREFIX, OPEN_COMPUTE_PREFIX, QUEUE_BODY_MAX_BYTES, QUEUE_DRAIN_MAX_BYTES, QUEUE_BACKEND_TIMEOUT_MS, BACKEND_TIMEOUT, drainToBuffer, backendCallWithTimeout } from "./queue-wire.js";
function queueError(message, status = 400) {
    return new Response(JSON.stringify({
        error: message
    }), {
        status,
        headers: {
            "content-type": "application/json"
        }
    });
}
export class QueueFacade extends WorkerEntrypoint {
    async fetch(request) {
        try {
            const props = resourceProps(this.ctx.props);
            const backend = wrapBindingBackend(this.env.BINDING_BACKEND, this.ctx.props);
            const url = new URL(request.url);
            const drained = await drainToBuffer(request.body, QUEUE_BODY_MAX_BYTES, QUEUE_DRAIN_MAX_BYTES);
            if (!drained.ok) {
                return queueError("queue request body too large", 413);
            }
            const leaf = url.pathname.split("/").filter(Boolean).pop() ?? "";
            const op = QUEUE_OPS.includes(leaf) ? leaf : null;
            const methodOk = request.method === "POST" && (op === "message" || op === "batch") || request.method === "GET" && op === "metrics";
            if (!methodOk || op === null) {
                return queueError("unsupported queue route", 404);
            }
            const headers = new Headers(request.headers);
            for (const key of [
                ...headers.keys()
            ]){
                const k = key.toLowerCase();
                if (k.startsWith(QUEUE_STRIP_HEADER_PREFIX) || k.startsWith(OPEN_COMPUTE_PREFIX)) {
                    headers.delete(key);
                }
            }
            headers.set("x-open-compute-request-id", crypto.randomUUID());
            const target = new URL(`${QUEUE_PROVIDER_PREFIX}${props.bindingId}/${op}${url.search}`, request.url).href;
            const providerMethod = "POST";
            const resp = await backendCallWithTimeout(backend, target, {
                method: providerMethod,
                headers,
                body: request.method === "GET" ? undefined : drained.body,
                redirect: "manual"
            }, QUEUE_BACKEND_TIMEOUT_MS);
            if (resp === BACKEND_TIMEOUT) {
                return queueError("queue backend timeout", 504);
            }
            if (resp instanceof Error) {
                return queueError("queue backend error", 502);
            }
            return resp;
        } catch  {
            return queueError("facade error", 500);
        }
    }
}
export default QueueFacade;
