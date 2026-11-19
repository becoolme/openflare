import { WorkerEntrypoint } from "cloudflare:workers";
import { drainToBuffer } from "./queue-wire.js";
const DELIVERY_TIMEOUT_MS = 55_000;
const DELIVERY_BUDGET_HEADER = "x-vrt-delivery-budget-ms";
const DELIVERY_TIMEOUT_MIN_MS = 1_000;
function deliveryTimeoutMs(raw) {
    if (raw === null || !/^[0-9]{1,9}$/.test(raw)) return DELIVERY_TIMEOUT_MS;
    const v = Number(raw);
    if (!Number.isSafeInteger(v) || v <= 0) return DELIVERY_TIMEOUT_MS;
    return Math.min(Math.max(v, DELIVERY_TIMEOUT_MIN_MS), DELIVERY_TIMEOUT_MS);
}
const DISPATCH_BODY_MAX = (4 * 1024 * 1024 + 128 * 1024) * 2 + 128_000;
const DISPATCH_DRAIN_MAX_BYTES = DISPATCH_BODY_MAX + 256_000;
function dispatchError(message, status = 400) {
    return new Response(JSON.stringify({
        outcome: "error",
        error: message
    }), {
        status,
        headers: {
            "content-type": "application/json"
        }
    });
}
function logReject(reason, fields) {
    const e = fields.err;
    const line = {
        event: "queue_dispatch_reject",
        reason,
        ...fields.queue !== undefined ? {
            queue: fields.queue.slice(0, 128)
        } : {},
        ...fields.messageCount !== undefined ? {
            messageCount: fields.messageCount
        } : {},
        ...fields.detail !== undefined ? {
            detail: fields.detail.slice(0, 128)
        } : {},
        ...e instanceof Error ? {
            errName: e.name.slice(0, 64),
            errMessage: e.message.slice(0, 256)
        } : e !== undefined ? {
            errMessage: String(e).slice(0, 256)
        } : {}
    };
    console.error(JSON.stringify(line));
}
function hexToBytes(hex) {
    if (hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) return null;
    const out = new Uint8Array(hex.length / 2);
    for(let i = 0; i < out.length; i++){
        out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    }
    return out;
}
function toQueueMessage(m) {
    if (typeof m.id !== "string" || m.id.length === 0 || m.id.length > 128) {
        return {
            msg: null,
            why: "id"
        };
    }
    const ts = typeof m.timestampMs === "number" && Number.isSafeInteger(m.timestampMs) ? m.timestampMs : null;
    if (ts === null) return {
        msg: null,
        why: "timestampMs"
    };
    const attempts = typeof m.attempts === "number" && Number.isSafeInteger(m.attempts) && m.attempts >= 1 ? m.attempts : 1;
    const raw = hexToBytes(typeof m.bodyHex === "string" ? m.bodyHex : "");
    if (raw === null) return {
        msg: null,
        why: "bodyHex"
    };
    const base = {
        id: m.id,
        timestamp: new Date(ts),
        attempts
    };
    switch(m.contentType){
        case "v8":
            return {
                msg: {
                    ...base,
                    serializedBody: raw.buffer
                }
            };
        case "bytes":
            return {
                msg: {
                    ...base,
                    body: raw.buffer
                }
            };
        case "json":
            try {
                return {
                    msg: {
                        ...base,
                        body: JSON.parse(new TextDecoder().decode(raw))
                    }
                };
            } catch  {
                return {
                    msg: null,
                    why: "json"
                };
            }
        case "text":
            return {
                msg: {
                    ...base,
                    body: new TextDecoder().decode(raw)
                }
            };
        default:
            return {
                msg: null,
                why: "contentType"
            };
    }
}
function normalizeResult(r) {
    const ids = (v)=>Array.isArray(v) ? v.filter((x)=>typeof x === "string" && x.length <= 128).slice(0, 100) : [];
    const rb = r.retryBatch ?? {};
    return {
        outcome: typeof r.outcome === "string" ? r.outcome.slice(0, 32) : "ok",
        ackAll: r.ackAll === true,
        retryBatch: {
            retry: rb.retry === true,
            ...typeof rb.delaySeconds === "number" && rb.delaySeconds >= 0 ? {
                delaySeconds: Math.min(Math.floor(rb.delaySeconds), 86400)
            } : {}
        },
        explicitAcks: ids(r.explicitAcks),
        explicitRetries: ids(r.explicitRetries),
        retryMessages: Array.isArray(r.retryMessages) ? r.retryMessages.filter((x)=>x !== null && typeof x === "object" && typeof x.msgId === "string").slice(0, 100).map((x)=>({
                msgId: x.msgId.slice(0, 128),
                ...typeof x.delaySeconds === "number" && x.delaySeconds >= 0 ? {
                    delaySeconds: Math.min(Math.floor(x.delaySeconds), 86400)
                } : {}
            })) : []
    };
}
export class QueueConsumerFacade extends WorkerEntrypoint {
    async fetch(request) {
        const drained = await drainToBuffer(request.body, DISPATCH_BODY_MAX, DISPATCH_DRAIN_MAX_BYTES);
        const seen = {
            queue: undefined,
            n: 0
        };
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
            let doc;
            try {
                doc = JSON.parse(new TextDecoder().decode(drained.body));
            } catch  {
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
                logReject("messages_invalid", {
                    queue: seen.queue
                });
                return dispatchError("dispatch messages invalid");
            }
            seen.n = rawMsgs.length;
            const mapped = rawMsgs.map((m)=>toQueueMessage(m));
            const firstBad = mapped.findIndex((r)=>r.msg === null);
            if (firstBad >= 0) {
                logReject("message_invalid", {
                    queue: seen.queue,
                    messageCount: seen.n,
                    detail: `index=${firstBad} field=${mapped[firstBad].why ?? "?"}`
                });
                return dispatchError("dispatch message invalid");
            }
            const messages = mapped.map((r)=>r.msg);
            const metadata = doc.metadata !== null && typeof doc.metadata === "object" ? doc.metadata : undefined;
            const consumer = this.env.CONSUMER;
            if (consumer === undefined || typeof consumer.queue !== "function") {
                logReject("binding_unavailable", {
                    queue: seen.queue,
                    messageCount: seen.n
                });
                return dispatchError("consumer binding unavailable", 503);
            }
            const deliveryTimeout = deliveryTimeoutMs(request.headers.get(DELIVERY_BUDGET_HEADER));
            let result;
            try {
                result = await Promise.race([
                    consumer.queue(queue, messages, metadata),
                    new Promise((_, reject)=>setTimeout(()=>reject(new Error("delivery timeout")), deliveryTimeout))
                ]);
            } catch (e) {
                const outcome = e instanceof Error && e.message === "delivery timeout" ? "timeout" : "exception";
                logReject(outcome === "timeout" ? "delivery_timeout" : "handler_exception", {
                    queue: seen.queue,
                    messageCount: seen.n,
                    err: e
                });
                return Response.json({
                    outcome,
                    ackAll: false,
                    retryBatch: {
                        retry: true
                    },
                    explicitAcks: [],
                    explicitRetries: [],
                    retryMessages: []
                });
            }
            const normalized = normalizeResult(result);
            if (normalized.outcome !== "ok") {
                logReject("handler_outcome", {
                    queue: seen.queue,
                    messageCount: seen.n,
                    detail: String(normalized.outcome).slice(0, 32)
                });
            }
            return Response.json(normalized);
        } catch (e) {
            logReject("facade_error", {
                queue: seen.queue,
                messageCount: seen.n,
                err: e
            });
            return dispatchError("facade error", 500);
        }
    }
}
export default QueueConsumerFacade;
