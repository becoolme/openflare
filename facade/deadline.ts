// RV-R71: shared facade -> provider call deadline. Every backend hop
// (D1, PG, queue) is bounded by `backendCallWithTimeout`; a hung
// provider/vrt-sql can never leave a tenant call dangling.
//
// Budgets (see doc/platform/workers-runtime-facade.md "Timeouts"):
//   queue 10s  (vrt-sql op budget 5s + slack)
//   D1/PG 25s  (below the provider 30s body+forward ceiling, so the
//               facade answers first with a typed retryable error)

export const D1_BACKEND_TIMEOUT_MS = 25_000;
export const PG_BACKEND_TIMEOUT_MS = 25_000;

/** Sentinel returned by `backendCallWithTimeout` on deadline. */
export const BACKEND_TIMEOUT = "backend_timeout" as const;
export type BackendCallResult = Response | typeof BACKEND_TIMEOUT | Error;

/**
 * Bounded backend call: races `fetcher.fetch` against a deadline and
 * signals cancellation via AbortSignal (a hint — a transport that
 * ignores signals still loses the race, so the facade never blocks on
 * a hung backend). Returns the Response, BACKEND_TIMEOUT on deadline,
 * or the thrown Error — never throws.
 */
export async function backendCallWithTimeout(
  fetcher: { fetch(input: unknown, init?: { [key: string]: unknown }): unknown },
  input: unknown,
  init: { [key: string]: unknown },
  timeoutMs: number,
): Promise<BackendCallResult> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort("backend timeout"), timeoutMs);
  try {
    const raced = await Promise.race([
      Promise.resolve()
        .then(() => fetcher.fetch(input, { ...init, signal: ctrl.signal }))
        .then((r) => ({ kind: "response" as const, value: r }))
        .catch((e: unknown) => ({
          kind: "error" as const,
          value: e instanceof Error ? e : new Error(String(e)),
        })),
      new Promise<{ kind: "timeout" }>((resolve) =>
        setTimeout(() => resolve({ kind: "timeout" }), timeoutMs),
      ),
    ]);
    if (raced.kind === "timeout") return BACKEND_TIMEOUT;
    if (raced.kind === "error") return raced.value;
    return raced.value instanceof Response
      ? raced.value
      : new Error("backend returned non-Response");
  } finally {
    clearTimeout(timer);
  }
}
