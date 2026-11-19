// Shared typed wire between `facade-queue` (the wrapped binding's inner
// service) and the provider's queue producer route — BOTH sides import
// these constants so the contract cannot silently fork (same discipline
// as d1-wire.ts, T31f/F0 lesson).
// Provider mirror: vrt-provider::server::parse_queue_path.

/// Provider route prefix: `/internal/bindings/v1/queue/{bindingId}/{op}`.
export const QUEUE_PROVIDER_PREFIX = "/internal/bindings/v1/queue/";

/// Ops accepted on the last path segment of the broker request
/// (workerd's queue binding emits `/message`, `/batch`, `/metrics`).
export const QUEUE_OPS = ["message", "batch", "metrics"] as const;
export type QueueOp = (typeof QUEUE_OPS)[number];

/// Everything under this prefix is host-controlled trusted context —
/// a tenant must never inject it; facade-queue strips ALL of them from
/// inbound requests before forwarding (S4 + T34 adversarial case).
export const QUEUE_STRIP_HEADER_PREFIX = "x-vrt-";

/// The provider identity channel (`x-open-compute-binding-id` etc.) —
/// same rule: host-injected only, inbound copies are stripped (T34d).
export const OPEN_COMPUTE_PREFIX = "x-open-compute-";

/// Inbound broker body cap — identical to vrt-sql `QUEUE_BODY_MAX`
/// (2*288_000 + 100*256 + 256 = 601_856): every legal broker batch fits;
/// anything larger is rejected at this layer instead of streaming
/// through. §8.
export const QUEUE_BODY_MAX_BYTES = 601_856;

/// Garbage-stream bound: workerd's queue producer never legitimately
/// exceeds QUEUE_BODY_MAX_BYTES. Past the cap the facade keeps reading
/// ONLY to satisfy the read-before-return invariant and abandons the
/// stream via cancel() at this hard multiple so a pathological source
/// cannot loop the drain forever. §8.
export const QUEUE_DRAIN_MAX_BYTES = QUEUE_BODY_MAX_BYTES * 4;

/// facade → provider call deadline (ms). Chosen above vrt-sql's 5s op
/// budget plus transport slack, and comfortably below any tenant
/// request budget — a hung backend yields a queue error, never a
/// dangling send (T37r2). §8.
export const QUEUE_BACKEND_TIMEOUT_MS = 10_000;

/**
 * Read-before-return invariant (T37r2 — real workerd measurement): the
 * queue producer keeps `env.Q.send*` pending until its request body has
 * been CONSUMED; a response returned with an unread stream hangs the
 * tenant forever (it never becomes QueueOperationError). The facade
 * therefore drains the inbound stream to EOF (or cancels past
 * drainMaxBytes) BEFORE returning anything — including the oversize
 * rejection itself.
 *
 * Returns {ok:true, body} when the stream fit inside maxBytes; {ok:false}
 * after the stream was fully drained/cancelled (oversize, corrupt, or
 * abandoned at the drain cap). Callers must not return early between
 * stream acquisition and this call completing.
 */
export async function drainToBuffer(
  stream: ReadableStream<Uint8Array> | null,
  maxBytes: number,
  drainMaxBytes: number,
): Promise<{ ok: true; body: ArrayBuffer } | { ok: false }> {
  if (stream === null) {
    return { ok: true, body: new ArrayBuffer(0) };
  }
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  let overflow = false;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (!overflow && total <= maxBytes) {
        chunks.push(value);
      } else {
        overflow = true;
      }
      if (total > drainMaxBytes) {
        // Pathological source — bound the drain, abandon the remainder.
        await reader.cancel("queue body drain cap").catch(() => {});
        break;
      }
    }
  } catch {
    // Broken inbound stream — nothing more to drain; treated as oversize
    // (an error, never a silent truncation).
    overflow = true;
  } finally {
    reader.releaseLock();
  }
  if (overflow) {
    return { ok: false };
  }
  const body = new Uint8Array(total);
  let off = 0;
  for (const chunk of chunks) {
    body.set(chunk, off);
    off += chunk.byteLength;
  }
  return { ok: true, body: body.buffer };
}

// RV-R71: the bounded-call helper is shared with D1/PG — re-exported
// here so existing queue imports keep working.
export {
  BACKEND_TIMEOUT,
  backendCallWithTimeout,
} from "./deadline.js";
