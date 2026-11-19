export const D1_BACKEND_TIMEOUT_MS = 25_000;
export const PG_BACKEND_TIMEOUT_MS = 25_000;
export const BACKEND_TIMEOUT = "backend_timeout";
export async function backendCallWithTimeout(fetcher, input, init, timeoutMs) {
    const ctrl = new AbortController();
    const timer = setTimeout(()=>ctrl.abort("backend timeout"), timeoutMs);
    try {
        const raced = await Promise.race([
            Promise.resolve().then(()=>fetcher.fetch(input, {
                    ...init,
                    signal: ctrl.signal
                })).then((r)=>({
                    kind: "response",
                    value: r
                })).catch((e)=>({
                    kind: "error",
                    value: e instanceof Error ? e : new Error(String(e))
                })),
            new Promise((resolve)=>setTimeout(()=>resolve({
                        kind: "timeout"
                    }), timeoutMs))
        ]);
        if (raced.kind === "timeout") return BACKEND_TIMEOUT;
        if (raced.kind === "error") return raced.value;
        return raced.value instanceof Response ? raced.value : new Error("backend returned non-Response");
    } finally{
        clearTimeout(timer);
    }
}
