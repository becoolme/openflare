// facade-qconsumer system worker — T37b consumer delivery bridge.
// vrt-host `/v1/queue/dispatch` proxies one batch here over
// `sock-qconsumer-<workerId>`; this worker forwards it to the
// consumer's `queue(batch)` handler via the `CONSUMER` service
// binding (`Fetcher.queue()` — the only workerd-internal way to
// deliver a queue event; capnp has no consumer wire type, cf.
// workerd.capnp :406 `queue @15 :ServiceDesignator` = producer only).
//
// Wire (dispatcher → facade, bounded at DISPATCH_BODY_MAX below):
//   POST /vrt/queue-dispatch
//   {"queue": "<queueName>",
//    "messages": [{"id","contentType","timestampMs","attempts","bodyHex"}],
//    "metadata": {"metrics": {...}}}
// Response (facade → dispatcher): QueueResult envelope —
//   {"outcome":"ok"|"exception"|"timeout",
//    "ackAll":bool,"retryBatch":{"retry":bool,"delaySeconds":?},
//    "explicitAcks":[],"explicitRetries":[],"retryMessages":[{msgId,delaySeconds}]}
// Missing fields are tolerated/normalized by the dispatcher — the
// envelope is best-effort, settlement semantics live caller-side.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  drainToBuffer,
} from "./queue-wire.js";

/// Per-batch consumer deadline — the dispatch claim_ttl budget sits
/// ABOVE this so a hung handler returns a settleable outcome instead
/// of leaking a dangling RPC (T37r2 lesson).
const DELIVERY_TIMEOUT_MS = 55_000;
/// Host-built (never client-supplied) remaining delivery budget: the
/// host subtracts the scale-to-zero wake time from the 55s budget.
/// Fail safe — missing / non-numeric / out of range is the full
/// default and the value can only SHRINK the timeout, never grow it.
const DELIVERY_BUDGET_HEADER = "x-vrt-delivery-budget-ms";
const DELIVERY_TIMEOUT_MIN_MS = 1_000;
function deliveryTimeoutMs(raw: string | null): number {
  if (raw === null || !/^[0-9]{1,9}$/.test(raw)) return DELIVERY_TIMEOUT_MS;
  const v = Number(raw);
  if (!Number.isSafeInteger(v) || v <= 0) return DELIVERY_TIMEOUT_MS;
  return Math.min(Math.max(v, DELIVERY_TIMEOUT_MIN_MS), DELIVERY_TIMEOUT_MS);
}
/// Body is a JSON envelope of hex-encoded messages. Worst-case legal
/// batch: claim max 100 × per-message body max 128_000B → 25.6MB hex.
/// Backlog accumulates ACROSS sends (the 288_000B cap only bounds one
/// /batch call), so the producer cap does NOT bind a dispatch batch —
/// a smaller cap here 413s a legal batch, which would retry until
/// maxRetries and land in the DLQ never delivered.
// T37b-r: dispatcher claim budget is 4MB RAW body bytes; the wire is
// hex (2×) plus per-message fields and envelope → (4MB + one
// oversize 128KB msg) × 2 + margin ≈ 8.8MB. The old 25.7MB cap came
// from count-only bounding — a byte budget now binds the claim, so
// each hop buffers ≤ ~8.8MB worst case instead of ~25.7MB.
const DISPATCH_BODY_MAX = (4 * 1024 * 1024 + 128 * 1024) * 2 + 128_000; // ~8.5MB
/// Drain cap slightly above the body cap — oversize envelopes are
/// still fully read/cancelled before responding (T37r2 invariant).
const DISPATCH_DRAIN_MAX_BYTES = DISPATCH_BODY_MAX + 256_000;

function dispatchError(message: string, status = 400): Response {
  return new Response(JSON.stringify({ outcome: "error", error: message }), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/// T37b-r: every error path logs ONE structured line — "不得静默吞错".
/// Fields are bounded and never carry message bodies (user data).
/// `console.error` is the only channel a system worker has into
/// workerd's own log; without it a consumer failure is invisible
/// outside the isolate.
function logReject(
  reason: string,
  fields: { queue?: string; messageCount?: number; err?: unknown; detail?: string },
): void {
  const e = fields.err;
  const line: Record<string, unknown> = {
    event: "queue_dispatch_reject",
    reason,
    ...(fields.queue !== undefined ? { queue: fields.queue.slice(0, 128) } : {}),
    ...(fields.messageCount !== undefined ? { messageCount: fields.messageCount } : {}),
    ...(fields.detail !== undefined ? { detail: fields.detail.slice(0, 128) } : {}),
    ...(e instanceof Error
      ? { errName: e.name.slice(0, 64), errMessage: e.message.slice(0, 256) }
      : e !== undefined
        ? { errMessage: String(e).slice(0, 256) }
        : {}),
  };
  console.error(JSON.stringify(line));
}

function hexToBytes(hex: string): Uint8Array | null {
  if (hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) return null;
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

/// Map a claimed vrt-sql message to the Fetcher.queue RPC shape —
/// byte-faithful per contentType (miniflare broker.worker.js:220-226:
/// `v8` → serializedBody bytes; text/json/bytes → decoded `body`).
function toQueueMessage(
  m: Record<string, unknown>,
): { msg: Record<string, unknown> | null; why?: string } {
  if (typeof m.id !== "string" || m.id.length === 0 || m.id.length > 128) {
    return { msg: null, why: "id" };
  }
  const ts = typeof m.timestampMs === "number" && Number.isSafeInteger(m.timestampMs)
    ? m.timestampMs : null;
  if (ts === null) return { msg: null, why: "timestampMs" };
  const attempts =
    typeof m.attempts === "number" && Number.isSafeInteger(m.attempts) && m.attempts >= 1
      ? m.attempts
      : 1;
  const raw = hexToBytes(typeof m.bodyHex === "string" ? m.bodyHex : "");
  if (raw === null) return { msg: null, why: "bodyHex" };
  const base = { id: m.id, timestamp: new Date(ts), attempts };
  switch (m.contentType) {
    case "v8":
      // v8-serialized bytes pass through verbatim — workerd
      // deserializes to the producer's original JS value.
      return { msg: { ...base, serializedBody: raw.buffer } };
    case "bytes":
      return { msg: { ...base, body: raw.buffer } };
    case "json":
      try {
        return { msg: { ...base, body: JSON.parse(new TextDecoder().decode(raw)) } };
      } catch {
        return { msg: null, why: "json" };
      }
    case "text":
      return { msg: { ...base, body: new TextDecoder().decode(raw) } };
    default:
      return { msg: null, why: "contentType" };
  }
}

/// QueueResult normalize — workerd's RPC result already carries these
/// members; only bounded copies leave the worker.
function normalizeResult(r: Record<string, unknown>): Record<string, unknown> {
  const ids = (v: unknown): string[] =>
    Array.isArray(v)
      ? v.filter((x): x is string => typeof x === "string" && x.length <= 128).slice(0, 100)
      : [];
  const rb = (r.retryBatch ?? {}) as Record<string, unknown>;
  return {
    outcome: typeof r.outcome === "string" ? r.outcome.slice(0, 32) : "ok",
    ackAll: r.ackAll === true,
    retryBatch: {
      retry: rb.retry === true,
      ...(typeof rb.delaySeconds === "number" && rb.delaySeconds >= 0
        ? { delaySeconds: Math.min(Math.floor(rb.delaySeconds), 86400) }
        : {}),
    },
    explicitAcks: ids(r.explicitAcks),
    explicitRetries: ids(r.explicitRetries),
    retryMessages: Array.isArray(r.retryMessages)
      ? (r.retryMessages as unknown[])
          .filter(
            (x): x is { msgId: string; delaySeconds?: number } =>
              x !== null &&
              typeof x === "object" &&
              typeof (x as { msgId?: unknown }).msgId === "string",
          )
          .slice(0, 100)
          .map((x) => ({
            msgId: x.msgId.slice(0, 128),
            ...(typeof x.delaySeconds === "number" && x.delaySeconds >= 0
              ? { delaySeconds: Math.min(Math.floor(x.delaySeconds), 86400) }
              : {}),
          }))
      : [],
  };
}

export class QueueConsumerFacade extends WorkerEntrypoint {
  async fetch(request: Request): Promise<Response> {
    // READ-BEFORE-RETURN (T37r2 invariant): drain before every return
    // path — the caller hangs on an unread body exactly like workerd's
    // producer path does.
    const drained = await drainToBuffer(
      request.body,
      DISPATCH_BODY_MAX,
      DISPATCH_DRAIN_MAX_BYTES,
    );
    const seen = { queue: undefined as string | undefined, n: 0 };
    try {
      if (!drained.ok) {
        logReject("body_too_large", {});
        return dispatchError("dispatch body too large", 413);
      }
      const url = new URL(request.url);
      if (request.method !== "POST" || url.pathname !== "/vrt/queue-dispatch") {
        logReject("bad_route", {});
        return dispatchError("unsupported dispatch route", 404);
      }
      let doc: Record<string, unknown>;
      try {
        doc = JSON.parse(new TextDecoder().decode(drained.body)) as Record<string, unknown>;
      } catch {
        logReject("not_json", {});
        return dispatchError("dispatch body is not json");
      }
      const queue = typeof doc.queue === "string" ? doc.queue : null;
      if (queue === null || queue.length === 0 || queue.length > 128) {
        logReject("queue_invalid", {});
        return dispatchError("dispatch queue name invalid");
      }
      seen.queue = queue;
      const rawMsgs = Array.isArray(doc.messages) ? doc.messages : null;
      if (rawMsgs === null || rawMsgs.length === 0 || rawMsgs.length > 100) {
        logReject("messages_invalid", { queue: seen.queue });
        return dispatchError("dispatch messages invalid");
      }
      seen.n = rawMsgs.length;
      const mapped = rawMsgs.map((m) =>
        toQueueMessage(m as Record<string, unknown>),
      );
      const firstBad = mapped.findIndex((r) => r.msg === null);
      if (firstBad >= 0) {
        logReject("message_invalid", {
          queue: seen.queue,
          messageCount: seen.n,
          detail: `index=${firstBad} field=${mapped[firstBad]!.why ?? "?"}`,
        });
        return dispatchError("dispatch message invalid");
      }
      const messages = mapped.map((r) => r.msg!);
      const metadata =
        doc.metadata !== null && typeof doc.metadata === "object"
          ? (doc.metadata as Record<string, unknown>)
          : undefined;
      const consumer = (
        this.env as { CONSUMER?: { queue(q: string, m: unknown[], md?: unknown): Promise<unknown> } }
      ).CONSUMER;
      if (consumer === undefined || typeof consumer.queue !== "function") {
        logReject("binding_unavailable", { queue: seen.queue, messageCount: seen.n });
        return dispatchError("consumer binding unavailable", 503);
      }
      const deliveryTimeout = deliveryTimeoutMs(request.headers.get(DELIVERY_BUDGET_HEADER));
      let result: unknown;
      try {
        result = await Promise.race([
          consumer.queue(queue, messages, metadata),
          new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error("delivery timeout")), deliveryTimeout),
          ),
        ]);
      } catch (e) {
        // Handler throw / isolate error / timeout — a settleable
        // outcome, never an HTTP failure: the dispatcher retries the
        // whole batch (miniflare exceptionQueueResponse parity —
        // outcome≠"ok" → retryAll). A timed-out handler may still be
        // running; at-least-once covers the duplicate.
        const outcome = e instanceof Error && e.message === "delivery timeout"
          ? "timeout"
          : "exception";
        logReject(outcome === "timeout" ? "delivery_timeout" : "handler_exception", {
          queue: seen.queue,
          messageCount: seen.n,
          err: e,
        });
        return Response.json({
          outcome,
          ackAll: false,
          retryBatch: { retry: true },
          explicitAcks: [],
          explicitRetries: [],
          retryMessages: [],
        });
      }
      const normalized = normalizeResult(result as Record<string, unknown>);
      // Handler RETURNED a non-ok outcome (thrown inside workerd's
      // queue() dispatch lands here too — workerd converts handler
      // exceptions to outcome:"exception" rather than rejecting the
      // RPC promise). Log so a persistently-failing consumer is
      // visible in workerd's log, not just retried forever.
      if (normalized.outcome !== "ok") {
        logReject("handler_outcome", {
          queue: seen.queue,
          messageCount: seen.n,
          detail: String(normalized.outcome).slice(0, 32),
        });
      }
      return Response.json(normalized);
    } catch (e) {
      logReject("facade_error", { queue: seen.queue, messageCount: seen.n, err: e });
      return dispatchError("facade error", 500);
    }
  }
}

export default QueueConsumerFacade;
