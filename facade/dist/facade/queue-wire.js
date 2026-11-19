export const QUEUE_PROVIDER_PREFIX = "/internal/bindings/v1/queue/";
export const QUEUE_OPS = [
    "message",
    "batch",
    "metrics"
];
export const QUEUE_STRIP_HEADER_PREFIX = "x-vrt-";
export const OPEN_COMPUTE_PREFIX = "x-open-compute-";
export const QUEUE_BODY_MAX_BYTES = 601_856;
export const QUEUE_DRAIN_MAX_BYTES = QUEUE_BODY_MAX_BYTES * 4;
export const QUEUE_BACKEND_TIMEOUT_MS = 10_000;
export async function drainToBuffer(stream, maxBytes, drainMaxBytes) {
    if (stream === null) {
        return {
            ok: true,
            body: new ArrayBuffer(0)
        };
    }
    const reader = stream.getReader();
    const chunks = [];
    let total = 0;
    let overflow = false;
    try {
        for(;;){
            const { done, value } = await reader.read();
            if (done) break;
            total += value.byteLength;
            if (!overflow && total <= maxBytes) {
                chunks.push(value);
            } else {
                overflow = true;
            }
            if (total > drainMaxBytes) {
                await reader.cancel("queue body drain cap").catch(()=>{});
                break;
            }
        }
    } catch  {
        overflow = true;
    } finally{
        reader.releaseLock();
    }
    if (overflow) {
        return {
            ok: false
        };
    }
    const body = new Uint8Array(total);
    let off = 0;
    for (const chunk of chunks){
        body.set(chunk, off);
        off += chunk.byteLength;
    }
    return {
        ok: true,
        body: body.buffer
    };
}
export { BACKEND_TIMEOUT, backendCallWithTimeout } from "./deadline.js";
