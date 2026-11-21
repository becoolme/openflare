// facade-d1 system worker — inner `fetcher` of the wrapped binding
// `cloudflare-internal:d1-api`. Receives `/query`/`/execute`/`/dump`
// POSTs from the d1-api implementation, injects signed props identity,
// and forwards to the provider's D1 route (facade-d1 → provider → vrt-sql).
//
// Trust boundary (T34): the request originates INSIDE workerd's d1-api,
// which relays the tenant Worker's call — so every inbound `x-vrt-*`
// header is user-controlled and MUST be stripped before forwarding;
// identity comes only from host-injected ctx.props (props.ts). The
// facade never reads the binding name — authorization names the
// declared binding via props.bindingId (T31f/F1 lesson).
import { WorkerEntrypoint } from "cloudflare:workers";
import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";
import {
  resourceProps,
  wrapBindingBackend,
} from "./props.js";
import {
  BACKEND_TIMEOUT,
  D1_BACKEND_TIMEOUT_MS,
  backendCallWithTimeout,
} from "./deadline.js";
import {
  D1_PROVIDER_PREFIX,
  D1_OPS,
  D1_STRIP_HEADER_PREFIX,
  OPEN_COMPUTE_PREFIX,
} from "./d1-wire.js";

/** D1 error surface: HTTP 200 + `{success:false,error:"D1_ERROR: …"}` —
 * the shape d1-api turns into a thrown `D1_ERROR` on the tenant side
 * (same contract as the provider's `d1_wire_error`). */
function d1Error(message: string, status = 200): Response {
  return new Response(
    JSON.stringify({ success: false, error: `D1_ERROR: ${message}` }),
    { status, headers: { "content-type": "application/json" } },
  );
}

/** `dump()` failure: d1-api treats ANY 200 body as the dump itself, so
 * every error path for `/dump` must carry a non-200 status. The body is
 * only diagnostic; workerd surfaces "D1_DUMP_ERROR: Status + <status>". */
function dumpError(status: number): Response {
  return new Response(
    JSON.stringify({ success: false, error: "D1_ERROR: dump failed" }),
    {
      status: status >= 400 ? status : 502,
      headers: { "content-type": "application/json", "cache-control": "no-store" },
    },
  );
}

interface FetchLike {
  fetch(input: unknown, init?: { headers?: unknown; [key: string]: unknown }): unknown;
}

export class D1Facade extends WorkerEntrypoint {
  async fetch(request: Request): Promise<Response> {
    // T34d: every failure inside this facade MUST surface as a D1
    // error body. d1-api turns non-ok HTTP into raw thrown text, and an
    // uncaught facade exception becomes workerd's opaque 500 page —
    // either way the operator sees "Internal Server Error" with no
    // cause. The structured detail lives in the provider's logs; the
    // tenant only gets a bounded reason string.
    //
    // `dump` is decided from the path before props parsing: its error
    // surface must be non-200 (a 200 body would be returned as the dump).
    const isDump =
      new URL(request.url).pathname.split("/").filter(Boolean).pop() === "dump";
    try {
      // Props are validated once inside wrapBindingBackend; parse once
      // more for the bindingId that anchors the provider path.
      const props = resourceProps(this.ctx.props);
      const backend = wrapBindingBackend(
        (this.env as { BINDING_BACKEND?: FetchLike }).BINDING_BACKEND,
        this.ctx.props,
      );
      const url = new URL(request.url);
      const leaf = url.pathname.split("/").filter(Boolean).pop() ?? "";
      const op = (D1_OPS as readonly string[]).includes(leaf) ? leaf : null;
      if (request.method !== "POST" || op === null) {
        return d1Error("unsupported d1 route", 404);
      }
      // Strip every user-controllable trusted-context header:
      // `x-vrt-*` is the vrt-sql channel (T34) and `x-open-compute-*`
      // is the provider identity channel — BOTH are host-set further
      // down the chain, so any inbound copy is forgery (T34d).
      const headers = new Headers(request.headers);
      for (const key of [...headers.keys()]) {
        const k = key.toLowerCase();
        if (k.startsWith(D1_STRIP_HEADER_PREFIX) || k.startsWith(OPEN_COMPUTE_PREFIX)) {
          headers.delete(key);
        }
      }
      // The provider's request-identity parse REQUIRES a per-request
      // RFC4122 `x-open-compute-request-id` (identity.rs — missing or
      // malformed is a silent BINDING_PROTOCOL_ERROR deny; T34d root
      // cause). Vendored transports mint one upstream; this facade
      // builds the request itself so it mints one here.
      headers.set("x-open-compute-request-id", crypto.randomUUID());
      // bindingId is validated by resourceProps (BINDING_ID_SAFE_RE:
      // no `/ ? # %` chars) — safe to interpolate into the path.
      const target = new URL(
        `${D1_PROVIDER_PREFIX}${props.bindingId}/${op}${url.search}`,
        request.url,
      ).href;
      // RV-R71: bounded — a hung provider/vrt-sql surfaces as a
      // retryable D1_ERROR (same 200 + D1_ERROR shape as every other
      // denial, so d1-api parses it) instead of a dangling call.
      const resp = await backendCallWithTimeout(
        backend,
        target,
        {
          method: "POST",
          headers,
          body: request.body,
          redirect: "manual",
        },
        D1_BACKEND_TIMEOUT_MS,
      );
      if (resp === BACKEND_TIMEOUT) {
        return isDump ? dumpError(504) : d1Error("backend timeout (504) - retryable");
      }
      if (resp instanceof Error) {
        return isDump ? dumpError(502) : d1Error("backend error (502) - retryable");
      }
      if (!(resp instanceof Response)) {
        return isDump ? dumpError(502) : d1Error("backend protocol error");
      }
      if (!resp.ok) {
        // Provider denial/5xx: bounded surface only — the provider's
        // structured warn log carries the reason code; the tenant
        // learns nothing beyond "denied".
        return isDump
          ? dumpError(resp.status)
          : d1Error(`backend denied request (${resp.status})`);
      }
      return resp;
    } catch {
      return isDump ? dumpError(500) : d1Error("facade error");
    }
  }
}

export default D1Facade;
