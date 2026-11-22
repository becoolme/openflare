// facade-k2 system worker — inner `fetcher` of the `cloudflare-internal:k2`
// wrapped binding. Receives `POST /send` (JSON `{records:[{content:b64,
// headers?}]}`) from the shim, injects signed props identity, and
// forwards to the provider's k2 route (facade-k2 → provider → vrt-sql).
//
// Trust boundary (K2, T37 precedent): every inbound `x-vrt-*`/
// `x-open-compute-*` header is user-controlled and MUST be stripped;
// identity comes only from host-injected ctx.props. The stream id
// never rides the request — routing uses props.bindingId only; the
// provider resolves binding → stream from signed desired state.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  resourceProps,
  wrapBindingBackend,
} from "./props.js";
import {
  K2_PROVIDER_PREFIX,
  K2_OPS,
  K2_STRIP_HEADER_PREFIX,
  OPEN_COMPUTE_PREFIX,
  K2_BODY_MAX_BYTES,
  K2_DRAIN_MAX_BYTES,
  K2_BACKEND_TIMEOUT_MS,
} from "./k2-wire.js";
import { drainToBuffer } from "./queue-wire.js";
import {
  BACKEND_TIMEOUT,
  backendCallWithTimeout,
} from "./deadline.js";

/** K2 error surface: `{error: {code, message, retryable}}` — the shim
 * turns this into `send()`'s non-throwing `{error}` result; a missing
 * `error.code` still carries the shape so the shim never throws on a
 * malformed platform reply. */
function k2Error(code: string, message: string, status = 400): Response {
  return new Response(JSON.stringify({ error: { code, message, retryable: false } }), {
    status,
    headers: { "content-type": "application/json" },
  });
}

interface FetchLike {
  fetch(input: unknown, init?: { headers?: unknown; [key: string]: unknown }): unknown;
}

export class K2Facade extends WorkerEntrypoint {
  async fetch(request: Request): Promise<Response> {
    // T34d discipline: every failure inside this facade surfaces as a
    // bounded k2 error body; the structured reason stays in the
    // provider's logs.
    try {
      const props = resourceProps(this.ctx.props);
      const backend = wrapBindingBackend(
        (this.env as { BINDING_BACKEND?: FetchLike }).BINDING_BACKEND,
        this.ctx.props,
      );
      const url = new URL(request.url);
      // READ-BEFORE-RETURN (T37r2): drain the inbound stream BEFORE any
      // return — an unread body can stall the caller; oversize drains to
      // EOF (bounded by DRAIN_MAX then cancel).
      const drained = await drainToBuffer(
        request.body,
        K2_BODY_MAX_BYTES,
        K2_DRAIN_MAX_BYTES,
      );
      if (!drained.ok) {
        return k2Error("BATCH_TOO_LARGE", "k2 request body too large", 413);
      }
      const leaf = url.pathname.split("/").filter(Boolean).pop() ?? "";
      const op = (K2_OPS as readonly string[]).includes(leaf) ? leaf : null;
      if (op === null || request.method !== "POST") {
        return k2Error("BAD_ROUTE", "unsupported k2 route", 404);
      }
      // Strip every user-controllable trusted-context header — inbound
      // copies are forgery (T34d).
      const headers = new Headers(request.headers);
      for (const key of [...headers.keys()]) {
        const k = key.toLowerCase();
        if (k.startsWith(K2_STRIP_HEADER_PREFIX) || k.startsWith(OPEN_COMPUTE_PREFIX)) {
          headers.delete(key);
        }
      }
      // Provider's identity parse REQUIRES a per-request RFC4122
      // x-open-compute-request-id (identity.rs); mint it here.
      headers.set("x-open-compute-request-id", crypto.randomUUID());
      const target = new URL(
        `${K2_PROVIDER_PREFIX}${props.bindingId}/${op}${url.search}`,
        request.url,
      ).href;
      const resp = await backendCallWithTimeout(
        backend,
        target,
        {
          method: "POST",
          headers,
          body: drained.body,
          redirect: "manual",
        },
        K2_BACKEND_TIMEOUT_MS,
      );
      if (resp === BACKEND_TIMEOUT) {
        return k2Error("BACKEND_TIMEOUT", "k2 backend timeout", 504);
      }
      if (resp instanceof Error) {
        return k2Error("BACKEND_ERROR", "k2 backend error", 502);
      }
      return resp;
    } catch {
      return k2Error("FACADE_ERROR", "facade error", 500);
    }
  }
}

export default K2Facade;
