import { bindingError } from "../loader/shared.js";
export function framed(metadata, body, limitCode) {
    let json;
    try {
        const encoded = JSON.stringify(metadata);
        if (encoded === undefined) throw bindingError(limitCode);
        json = encoded;
    } catch  {
        throw bindingError(limitCode);
    }
    const value = new TextEncoder().encode(json);
    if (value.byteLength > 64 * 1024) throw bindingError(limitCode);
    const prefix = new Uint8Array(value.byteLength + 4);
    new DataView(prefix.buffer).setUint32(0, value.byteLength, false);
    prefix.set(value, 4);
    if (!body) {
        return new ReadableStream({
            start (controller) {
                controller.enqueue(prefix);
                controller.close();
            }
        });
    }
    const reader = body.getReader();
    let sent = false;
    return new ReadableStream({
        async pull (controller) {
            if (!sent) {
                sent = true;
                controller.enqueue(prefix);
                return;
            }
            const part = await reader.read();
            if (part.done) controller.close();
            else controller.enqueue(part.value);
        },
        cancel (reason) {
            return reader.cancel(reason);
        }
    });
}
export async function expectBindingStatus(response, status, code) {
    if (response.status === status) return;
    try {
        await response.body?.cancel();
    } catch  {}
    throw bindingError(code);
}
export async function bindingJson(response, code) {
    try {
        return await response.json();
    } catch  {
        throw bindingError(code);
    }
}
export function isRecord(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
