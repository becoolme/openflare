import { WorkerEntrypoint } from "cloudflare:workers";
import { resourceProps, wrapBindingBackend } from "./props.js";
import { EMAIL_PROVIDER_PREFIX, EMAIL_OPS, EMAIL_STRIP_HEADER_PREFIX, OPEN_COMPUTE_PREFIX, EMAIL_BODY_MAX_BYTES, EMAIL_DRAIN_MAX_BYTES, EMAIL_BACKEND_TIMEOUT_MS } from "./email-wire.js";
import { drainToBuffer } from "./queue-wire.js";
import { BACKEND_TIMEOUT, backendCallWithTimeout } from "./deadline.js";
function emailError(code, message, status = 400) {
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
export class EmailFacade extends WorkerEntrypoint {
    async fetch(request) {
        try {
            const props = resourceProps(this.ctx.props);
            const backend = wrapBindingBackend(this.env.BINDING_BACKEND, this.ctx.props);
            const url = new URL(request.url);
            const drained = await drainToBuffer(request.body, EMAIL_BODY_MAX_BYTES, EMAIL_DRAIN_MAX_BYTES);
            if (!drained.ok) {
                return emailError("E_CONTENT_TOO_LARGE", "email request body too large", 413);
            }
            const leaf = url.pathname.split("/").filter(Boolean).pop() ?? "";
            const op = EMAIL_OPS.includes(leaf) ? leaf : null;
            if (op === null || request.method !== "POST") {
                return emailError("E_VALIDATION_ERROR", "unsupported email route", 404);
            }
            const headers = new Headers(request.headers);
            for (const key of [
                ...headers.keys()
            ]){
                const k = key.toLowerCase();
                if (k.startsWith(EMAIL_STRIP_HEADER_PREFIX) || k.startsWith(OPEN_COMPUTE_PREFIX)) {
                    headers.delete(key);
                }
            }
            headers.set("x-open-compute-request-id", crypto.randomUUID());
            const target = new URL(`${EMAIL_PROVIDER_PREFIX}${props.bindingId}/${op}${url.search}`, request.url).href;
            const resp = await backendCallWithTimeout(backend, target, {
                method: "POST",
                headers,
                body: drained.body,
                redirect: "manual"
            }, EMAIL_BACKEND_TIMEOUT_MS);
            if (resp === BACKEND_TIMEOUT) {
                return emailError("E_INTERNAL_SERVER_ERROR", "email backend timeout", 504);
            }
            if (resp instanceof Error) {
                return emailError("E_INTERNAL_SERVER_ERROR", "email backend error", 502);
            }
            return resp;
        } catch  {
            return emailError("E_INTERNAL_SERVER_ERROR", "facade error", 500);
        }
    }
}
export default EmailFacade;
