// Shared egress constants — a NON-entry helper module.
//
// T25/L12: real workerd requires every NAMED export of a worker's
// main module to be a handler (function / ExportedHandler class).
// `export const` values in an entry module become invalid service-map
// entries and the whole service fails to load:
//   service vrt-egress: Incorrect type for map entry 'DENIED_HEADER'
// Constants therefore live here, imported by the entry modules —
// helper-module exports never reach the service map.
export const POLICY_BINDING = "VRT_EGRESS_POLICY";
export const DENIED_HEADER = "x-vrt-egress-denied";
export const ERROR_HEADER = "x-vrt-egress-error";
export const DENY_CODE = "no_outbound";

// Platform-internal marker namespaces must never leave the pod: they
// signal trusted identity to our own services, are meaningless outside,
// and an external host must not be able to spoof them back at the
// tenant either.
const INTERNAL_PREFIXES = ["x-vrt-", "x-open-compute-"];

export function stripInternalHeaders(headers: Headers): Headers {
  const out = new Headers();
  headers.forEach((value, key) => {
    const lower = key.toLowerCase();
    if (INTERNAL_PREFIXES.some((p) => lower.startsWith(p))) return;
    if (lower === "host") return;
    out.append(key, value);
  });
  return out;
}

// MIG-10: upstream-failure classification + structured logging. The
// previous `catch {}` swallowed every upstream error into a synthetic
// 502 — unobservable and CF-incompatible (on Cloudflare an unreachable
// upstream REJECTS the tenant's fetch(), it does not answer 502).
//
// Structured logging convention: one JSON line on console.error —
// error class, target host+port, elapsed ms. NEVER the request path,
// query, userinfo, headers, or response body: workerd error messages
// can embed the full URL including credentials.

export type UpstreamErrorClass =
  | "tls"
  | "connect"
  | "dns"
  | "timeout"
  | "aborted"
  | "other";

/// Classify an upstream `fetch()` rejection by name/message. Order
/// matters: timeout before aborted (AbortSignal.timeout() yields
/// TimeoutError on modern runtimes, AbortError on older ones), tls
/// before connect ("certificate is not trusted" also says connect).
export function classifyUpstreamError(err: unknown): UpstreamErrorClass {
  const name = err instanceof Error ? err.name : "";
  const msg = (err instanceof Error ? err.message : String(err ?? "")).toLowerCase();
  if (name === "TimeoutError" || /timed?[\s_-]?out|timeout/.test(msg)) return "timeout";
  if (name === "AbortError") return "aborted";
  if (/certificate|cert\b|tls|ssl|handshake|issuer|self.?signed|x509/.test(msg)) return "tls";
  if (/dns|resolv|getaddrinfo|enotfound|name or service|nxdomain/.test(msg)) return "dns";
  if (/connect|econn|refus|unreach|reset|restrictpeers|socket|forbidden|denied/.test(msg)) return "connect";
  return "other";
}

/// host+port of a request URL — the ONLY target fields safe to log.
/// `URL.hostname`/`port` carry no path, query, or userinfo by
/// construction; an unparseable URL yields empty values.
export function upstreamEndpoint(url: string): { host: string; port: number | null } {
  try {
    const u = new URL(url);
    const port =
      u.port === "" ? (u.protocol === "https:" ? 443 : u.protocol === "http:" ? 80 : null) : Number(u.port);
    return { host: u.hostname, port };
  } catch {
    return { host: "", port: null };
  }
}

/// One structured stderr line for an upstream failure. The error's
/// own message is deliberately NOT logged — workerd messages may
/// embed the attempted URL (with credentials) or internal addresses.
export function logUpstreamError(
  cls: UpstreamErrorClass,
  url: string,
  elapsedMs: number,
  err: unknown,
): void {
  const ep = upstreamEndpoint(url);
  console.error(
    JSON.stringify({
      event: "egress_upstream_error",
      class: cls,
      host: ep.host.slice(0, 253),
      port: ep.port,
      ms: Math.max(0, Math.round(elapsedMs)),
      errName: err instanceof Error ? err.name.slice(0, 64) : typeof err,
    }),
  );
}

/// The error thrown back at the tenant — CF semantics: unreachable
/// upstream rejects `fetch()`, it never answers a synthetic 502.
/// The message carries the stable class code and nothing about the
/// internal target (the tenant already knows its own URL).
export function upstreamFetchError(cls: UpstreamErrorClass): Error {
  const msg = `vrt egress upstream: ${cls}`;
  // Timeout stays TimeoutError — closest to CF's AbortError/timeout
  // rejection shape for `AbortSignal.timeout()` callers. `stableCode`
  // mirrors connect()'s marker so tenants can branch programmatically
  // without parsing the message.
  const err = cls === "timeout" ? new DOMException(msg, "TimeoutError") : new Error(msg);
  return Object.assign(err, { stableCode: "EGRESS_UPSTREAM", egressClass: cls });
}

// MIG-14: raw-TCP connect() helpers. Logging follows the same
// convention as logUpstreamError — one JSON line per event on
// console.error, fields chosen for metric aggregation (port, code,
// reason), and NEVER the target hostname: plaintext names would be a
// high-cardinality leak of internal DNS and tenant intent.

/** A denied connect(): typed code + target port, no hostname. */
export function logTcpDeny(port: number | null, code: string): void {
  console.error(JSON.stringify({ event: "egress_tcp_deny", port, code }));
}

/** Session end: byte counters + lifetime + teardown reason. */
export function logTcpClose(
  port: number,
  upBytes: number,
  downBytes: number,
  elapsedMs: number,
  reason: string,
): void {
  console.error(
    JSON.stringify({
      event: "egress_tcp_close",
      port,
      upBytes,
      downBytes,
      ms: Math.max(0, Math.round(elapsedMs)),
      reason,
    }),
  );
}

/** The typed error thrown at the tenant for a denied/failed connect().
 *  Same marker pair as the fetch deny body (`stableCode` + `denyCode`),
 *  surfaced on the socket's rejection and its `closed` promise. */
export function tcpDenied(code: string, detail?: string): Error {
  return Object.assign(new Error(`vrt egress denies connect(): ${code}`), {
    stableCode: "EGRESS_DENIED",
    denyCode: code,
    ...(detail === undefined ? {} : { detail }),
  });
}

/** Best-effort socket close — never throws. */
export async function closeQuietly(
  socket: { close?: () => unknown } | null | undefined,
): Promise<void> {
  try {
    await socket?.close?.();
  } catch {
    // the socket is already dead — nothing left to do
  }
}

// Policy-checked outbound fetch for tenant-specified payload URLs
// (draw overlays, text fonts). Unlike the tenant passthrough — which
// returns 3xx to the tenant so each hop re-runs the policy — these
// fetches resolve redirects in-worker, so EVERY hop must re-run the
// target check: an allowlisted A 302-ing to a denyNames B must not
// fetch B. Layer-1 restrictPeers still guards the resolved IP at
// dial; this layer guards names and ports.
//
// Returns the first non-redirect response, or undefined on deny,
// transport failure, bad location, or too many hops.
export async function policyFetch(
  url: string,
  allowTarget: (url: string) => boolean,
  timeoutMs: number,
  maxHops = 5,
): Promise<Response | undefined> {
  let target = url;
  for (let hop = 0; ; hop++) {
    if (!allowTarget(target)) return undefined;
    let resp: Response;
    try {
      resp = await fetch(target, {
        redirect: "manual",
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch {
      return undefined;
    }
    if (resp.status < 300 || resp.status >= 400) return resp;
    const loc = resp.headers.get("location");
    if (loc === null || hop >= maxHops) return undefined;
    try {
      target = new URL(loc, target).toString();
    } catch {
      return undefined;
    }
    resp.body?.cancel().catch(() => {});
  }
}

// Drain a response body under a hard cap — payload fetchers must not
// buffer unbounded bodies before checking size. Returns undefined
// once `max` bytes are exceeded (the content-length fast path skips
// the read entirely); transport errors mid-stream throw like
// arrayBuffer() would.
export async function readCappedBody(
  resp: Response,
  max: number,
): Promise<Uint8Array | undefined> {
  const declared = Number(resp.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > max) {
    resp.body?.cancel().catch(() => {});
    return undefined;
  }
  if (resp.body === null) return new Uint8Array(0);
  const reader = resp.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > max) {
      reader.cancel().catch(() => {});
      return undefined;
    }
    chunks.push(value);
  }
  const out = new Uint8Array(total);
  let off = 0;
  for (const chunk of chunks) {
    out.set(chunk, off);
    off += chunk.byteLength;
  }
  return out;
}
