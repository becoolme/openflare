// facade-queue system worker — inner `service` of the native workerd
// `queue` binding. Receives `/message` + `/batch` (POST) and `/metrics`
// (GET) broker requests from workerd's queue producer implementation,
// injects signed props identity, and forwards to the provider's queue
// route (facade-queue → provider → vrt-sql).
//
// Trust boundary (T37): the request originates INSIDE workerd's queue
// producer path, which relays the tenant Worker's `env.Q.send*` call —
// so every inbound `x-vrt-*`/`x-open-compute-*` header is user-controlled
// and MUST be stripped before forwarding; identity comes only from
// host-injected ctx.props (props.ts). Routing uses props.bindingId ONLY —
// broker headers (`x-msg-*`, `cf-queue-*`) are forwarded verbatim to the
// provider, which revalidates them; no queue name/id is ever taken from
// the request (§8).
import { WorkerEntrypoint } from "cloudflare:workers";
import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";
import {
  resourceProps,
  wrapBindingBackend,
} from "./props.js";
import {
  QUEUE_PROVIDER_PREFIX,
  QUEUE_OPS,
  QUEUE_STRIP_HEADER_PREFIX,
  QUEUE_HDR_MSG_DELAY,
  OPEN_COMPUTE_PREFIX,
  QUEUE_BODY_MAX_BYTES,
  QUEUE_DRAIN_MAX_BYTES,
  QUEUE_BACKEND_TIMEOUT_MS,
  BACKEND_TIMEOUT,
  drainToBuffer,
  backendCallWithTimeout,
} from "./queue-wire.js";

/** Queue error surface: `{error: "…"}` with an HTTP status — the shape
 * workerd's queue binding turns into a thrown QueueOperationError on
 * the tenant side (miniflare broker `httpError`, §8/B:50). */
function queueError(message: string, status = 400): Response {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: { "content-type": "application/json" },
  });
}

interface FetchLike {
  fetch(input: unknown, init?: { headers?: unknown; [key: string]: unknown }): unknown;
}

/// Cloudflare `delivery_delay` bound (seconds).
const MAX_DELIVERY_DELAY_SECS = 86_400;

/**
 * Queue-level default send delay from host-injected props
 * (`deliveryDelaySeconds`, rendered only when the queue declares
 * `delivery_delay`). `null` = no queue-level default.
 *
 * The value is part of the signed snapshot and rendered by vrt-config; a
 * malformed value means the props contract is broken, so it fails closed
 * with a typed binding error instead of silently changing delivery
 * semantics.
 */
function queueDefaultDelay(rawProps: unknown): number | null {
  if (rawProps === null || typeof rawProps !== "object") return null;
  const value = (rawProps as Record<string, unknown>).deliveryDelaySeconds;
  if (value === undefined) return null;
  if (
    !Number.isSafeInteger(value) ||
    (value as number) < 0 ||
    (value as number) > MAX_DELIVERY_DELAY_SECS
  ) {
    throw bindingError("BINDING_PROTOCOL_ERROR");
  }
  return value as number;
}

export class QueueFacade extends WorkerEntrypoint {
  async fetch(request: Request): Promise<Response> {
    // T34d discipline: every failure inside this facade surfaces as a
    // bounded queue error body; the structured reason stays in the
    // provider's logs.
    try {
      const props = resourceProps(this.ctx.props);
      const backend = wrapBindingBackend(
        (this.env as { BINDING_BACKEND?: FetchLike }).BINDING_BACKEND,
        this.ctx.props,
      );
      const url = new URL(request.url);
      // READ-BEFORE-RETURN (T37r2, real workerd measurement): the queue
      // producer keeps `env.Q.send*` pending until its request body is
      // fully consumed — ANY response returned with an unread body
      // (404/413/500/backend error alike) hangs the tenant FOREVER
      // instead of throwing QueueOperationError. The inbound body is
      // therefore drained BEFORE every return path below — including
      // the route rejection itself. Oversize drains to EOF (bounded by
      // DRAIN_MAX then cancel); a garbage stream can still only stall
      // us for a bounded read, never hang the tenant.
      const drained = await drainToBuffer(
        // Always the real stream (never gated on method) — a GET
        // carrying a body must drain too, or the same hang returns.
        // `request.body` is null for bodyless requests → no-op.
        request.body,
        QUEUE_BODY_MAX_BYTES,
        QUEUE_DRAIN_MAX_BYTES,
      );
      if (!drained.ok) {
        return queueError("queue request body too large", 413);
      }
      const leaf = url.pathname.split("/").filter(Boolean).pop() ?? "";
      const op = (QUEUE_OPS as readonly string[]).includes(leaf) ? leaf : null;
      const methodOk =
        (request.method === "POST" && (op === "message" || op === "batch")) ||
        (request.method === "GET" && op === "metrics");
      if (!methodOk || op === null) {
        return queueError("unsupported queue route", 404);
      }
      // Strip every user-controllable trusted-context header — inbound
      // copies are forgery (T34d). Broker protocol headers (`x-msg-*`,
      // `cf-queue-*`) pass through; the provider validates them.
      const headers = new Headers(request.headers);
      for (const key of [...headers.keys()]) {
        const k = key.toLowerCase();
        if (k.startsWith(QUEUE_STRIP_HEADER_PREFIX) || k.startsWith(OPEN_COMPUTE_PREFIX)) {
          headers.delete(key);
        }
      }
      // Queue-level default delivery delay (wrangler `delivery_delay`):
      // workerd sets `X-Msg-Delay-Secs` ONLY when the caller passed a
      // delay (including an explicit 0), so an absent header means "no
      // per-send delay" — fill it with the queue default. An explicit
      // per-message `delaySeconds` therefore always wins; for
      // `/batch` the header is the batch-wide default and each item's
      // explicit `delaySecs` still overrides it in the provider.
      const defaultDelay = queueDefaultDelay(this.ctx.props);
      if (
        defaultDelay !== null &&
        (op === "message" || op === "batch") &&
        !headers.has(QUEUE_HDR_MSG_DELAY)
      ) {
        headers.set(QUEUE_HDR_MSG_DELAY, String(defaultDelay));
      }
      // Provider's identity parse REQUIRES a per-request RFC4122
      // x-open-compute-request-id (identity.rs); mint it here.
      headers.set("x-open-compute-request-id", crypto.randomUUID());
      // bindingId is validated by resourceProps (BINDING_ID_SAFE_RE:
      // no `/ ? # %` chars) — safe to interpolate into the path.
      const target = new URL(
        `${QUEUE_PROVIDER_PREFIX}${props.bindingId}/${op}${url.search}`,
        request.url,
      ).href;
      // The provider's tenant dispatch is POST-only for all binding
      // protocols (T13 gate) — broker `GET /metrics` is translated to
      // POST on the provider hop; the wire contract (`{metadata…}` /
      // metrics body) is unchanged.
      const providerMethod = "POST";
      // Bounded backend call — a hung provider/vrt-sql yields a queue
      // error at QUEUE_BACKEND_TIMEOUT_MS (< tenant request budget),
      // never a dangling send. §8.
      const resp = await backendCallWithTimeout(
        backend,
        target,
        {
          method: providerMethod,
          headers,
          body: request.method === "GET" ? undefined : drained.body,
          redirect: "manual",
        },
        QUEUE_BACKEND_TIMEOUT_MS,
      );
      if (resp === BACKEND_TIMEOUT) {
        return queueError("queue backend timeout", 504);
      }
      if (resp instanceof Error) {
        return queueError("queue backend error", 502);
      }
      return resp;
    } catch {
      return queueError("facade error", 500);
    }
  }
}

export default QueueFacade;
