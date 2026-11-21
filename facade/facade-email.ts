// facade-email system worker — inner `fetcher` of the
// `cloudflare-internal:send-email` wrapped binding. Receives
// `POST /send` (JSON EmailMessageBuilder) from the shim, injects signed
// props identity, and forwards to the provider's email route
// (facade-email → provider → configured upstream adapter).
//
// Trust boundary (T37 precedent): every inbound `x-vrt-*`/
// `x-open-compute-*` header is user-controlled and MUST be stripped;
// identity comes only from host-injected ctx.props. The binding's
// address restrictions never ride the request — the provider enforces
// them from signed desired state.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  resourceProps,
  wrapBindingBackend,
} from "./props.js";
import {
  EMAIL_PROVIDER_PREFIX,
  EMAIL_OPS,
  EMAIL_STRIP_HEADER_PREFIX,
  OPEN_COMPUTE_PREFIX,
  EMAIL_BODY_MAX_BYTES,
  EMAIL_DRAIN_MAX_BYTES,
  EMAIL_BACKEND_TIMEOUT_MS,
} from "./email-wire.js";
import { drainToBuffer } from "./queue-wire.js";
import {
  BACKEND_TIMEOUT,
  backendCallWithTimeout,
} from "./deadline.js";

/** Email error surface: `{error: {code, message, retryable}}` — the
 * shim turns any non-2xx into a thrown Error carrying `.code`; a
 * missing `error.code` still carries the shape so the shim never has
 * to guess. */
function emailError(code: string, message: string, status = 400): Response {
  return new Response(JSON.stringify({ error: { code, message, retryable: false } }), {
    status,
    headers: { "content-type": "application/json" },
  });
}

interface FetchLike {
  fetch(input: unknown, init?: { headers?: unknown; [key: string]: unknown }): unknown;
}

export class EmailFacade extends WorkerEntrypoint {
  async fetch(request: Request): Promise<Response> {
    // T34d discipline: every failure inside this facade surfaces as a
    // bounded error body; the structured reason stays in the provider's
    // logs.
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
        EMAIL_BODY_MAX_BYTES,
        EMAIL_DRAIN_MAX_BYTES,
      );
      if (!drained.ok) {
        return emailError("E_CONTENT_TOO_LARGE", "email request body too large", 413);
      }
      const leaf = url.pathname.split("/").filter(Boolean).pop() ?? "";
      const op = (EMAIL_OPS as readonly string[]).includes(leaf) ? leaf : null;
      if (op === null || request.method !== "POST") {
        return emailError("E_VALIDATION_ERROR", "unsupported email route", 404);
      }
      // Strip every user-controllable trusted-context header — inbound
      // copies are forgery (T34d).
      const headers = new Headers(request.headers);
      for (const key of [...headers.keys()]) {
        const k = key.toLowerCase();
        if (k.startsWith(EMAIL_STRIP_HEADER_PREFIX) || k.startsWith(OPEN_COMPUTE_PREFIX)) {
          headers.delete(key);
        }
      }
      // Provider's identity parse REQUIRES a per-request RFC4122
      // x-open-compute-request-id (identity.rs); mint it here.
      headers.set("x-open-compute-request-id", crypto.randomUUID());
      const target = new URL(
        `${EMAIL_PROVIDER_PREFIX}${props.bindingId}/${op}${url.search}`,
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
        EMAIL_BACKEND_TIMEOUT_MS,
      );
      if (resp === BACKEND_TIMEOUT) {
        return emailError("E_INTERNAL_SERVER_ERROR", "email backend timeout", 504);
      }
      if (resp instanceof Error) {
        return emailError("E_INTERNAL_SERVER_ERROR", "email backend error", 502);
      }
      return resp;
    } catch {
      return emailError("E_INTERNAL_SERVER_ERROR", "facade error", 500);
    }
  }
}

export default EmailFacade;
