// facade-pg system worker — P1 real implementation. The tenant's PG
// binding (env.DB) resolves to this entrypoint with host-injected
// ctx.props; every call forwards to the provider's PG route over
// BINDING_BACKEND. The tenant credential (JWT) lives ONLY provider-
// side — this facade never sees it; the isolate never sees it.
//
// Wire contract: POST /internal/bindings/v1/pg/<bindingId>/sql with a
// Neon-compatible body ({query,params} | {queries:[...]}). Read-only
// enforcement is provider-side + database-side (BEGIN READ ONLY) —
// the facade deliberately passes the body's shape through unchanged
// except stripping forgery headers.
//
// API surface for tenant code:
//   env.DB.query(sql, params?)            → single-statement result
//   env.DB.execute(sql, params?)          → alias of query (Neon parity)
//   env.DB.transaction([{sql,params}],opts?) → batch result (neon wire `results`)
//   env.DB.fetch                          → fetchFunction for neon():
//       neon("", { fetchFunction: env.DB.fetch })
//   env.DB.connect()                      → BINDING_CAPABILITY_UNSUPPORTED (TCP is T-future)
import { WorkerEntrypoint } from "cloudflare:workers";
import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";
import {
  BACKEND_TIMEOUT,
  PG_BACKEND_TIMEOUT_MS,
  backendCallWithTimeout,
} from "./deadline.js";
import {
  resourceProps,
  wrapBindingBackend,
  type ResourceProps,
} from "./props.js";

const PG_PROVIDER_PREFIX = "/internal/bindings/v1/pg/";
const OPEN_COMPUTE_PREFIX = "x-open-compute-";
// Inbound `x-vrt-*` (provider-internal channel) is likewise forgery
// here — the provider mints what it needs.
const VRT_PREFIX = "x-vrt-";

const MAX_BODY_BYTES = 256 * 1024;
const MAX_SQL_CHARS = 64 * 1024;
const MAX_PARAMS = 255;
const MAX_STATEMENTS = 64;
const MAX_PARAM_DEPTH = 16;

interface FetchLike {
  fetch(input: unknown, init?: { headers?: unknown; [key: string]: unknown }): unknown;
}

function pgError(code: string, retryable = false, resultUnknown = false): Response {
  return new Response(JSON.stringify({ ok: false, error: { code, retryable, resultUnknown } }), {
    status: 422,
    headers: { "content-type": "application/json", "x-open-compute-error-code": code },
  });
}

/** Parse the provider's typed error surface; anything else maps to
 * BINDING_PROTOCOL_ERROR (never echo upstream bodies verbatim). */
/** RV-R71: bounded provider hop; timeout/transport error -> stable,
 * retryable binding codes (never a dangling call, never fake success). */
async function boundedFetch(
  backend: { fetch(input: unknown, init?: { [key: string]: unknown }): unknown },
  target: string,
  init: { [key: string]: unknown },
): Promise<Response> {
  const resp = await backendCallWithTimeout(backend, target, init, PG_BACKEND_TIMEOUT_MS);
  if (resp === BACKEND_TIMEOUT) throw bindingError("BINDING_BACKEND_TIMEOUT");
  if (resp instanceof Error) throw bindingError("BINDING_BACKEND_UNAVAILABLE");
  return resp;
}

function backendError(resp: Response): never {
  // Header carries the stable code even when the body is opaque.
  const code = resp.headers.get("x-open-compute-error-code") ?? "BINDING_PROTOCOL_ERROR";
  throw bindingError(code);
}

/** Depth/size-bound a Neon params array — params travel the protocol,
 * never string-interpolation, but an absurd shape is a protocol error. */
function boundValue(value: unknown, depth: number): unknown {
  if (depth > MAX_PARAM_DEPTH) throw bindingError("BINDING_LIMIT_EXCEEDED");
  if (Array.isArray(value)) {
    return value.map((v) => boundValue(v, depth + 1));
  }
  if (value !== null && typeof value === "object") {
    // Typed JSON mapping is the gateway's job — pass plain objects only.
    if (Object.getPrototypeOf(value) !== Object.prototype) {
      throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) out[k] = boundValue(v, depth + 1);
    return out;
  }
  if (typeof value === "string" && value.length > MAX_BODY_BYTES) {
    throw bindingError("BINDING_LIMIT_EXCEEDED");
  }
  return value;
}

function boundParams(params: unknown): unknown[] {
  if (params === undefined || params === null) return [];
  if (!Array.isArray(params)) throw bindingError("BINDING_PROTOCOL_ERROR");
  if (params.length > MAX_PARAMS) throw bindingError("BINDING_LIMIT_EXCEEDED");
  return params.map((p) => boundValue(p, 0));
}

function boundSql(sql: unknown): string {
  if (typeof sql !== "string" || sql.length === 0 || sql.length > MAX_SQL_CHARS) {
    throw bindingError("BINDING_PROTOCOL_ERROR");
  }
  return sql;
}

export class PgTransport extends WorkerEntrypoint {
  readonly #props: ResourceProps;
  readonly #backend: FetchLike;

  constructor(ctx: { props?: unknown }, env: unknown) {
    super(ctx, env);
    this.#props = resourceProps(ctx?.props);
    this.#backend = wrapBindingBackend(
      (env as { BINDING_BACKEND?: FetchLike } | undefined)?.BINDING_BACKEND,
      ctx?.props,
    );
  }

  /** Forward one Neon wire payload to the provider's /sql route.
   * The body is canonical JSON built HERE — no worker bytes are
   * forwarded verbatim, so request smuggling is impossible. */
  async #post(body: Record<string, unknown>): Promise<unknown> {
    const target = new URL(
      `${PG_PROVIDER_PREFIX}${this.#props.bindingId}/sql`,
      "http://pg.internal",
    ).href;
    const resp = await boundedFetch(this.#backend, target, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-open-compute-request-id": crypto.randomUUID(),
      },
      body: JSON.stringify(body),
    });
    if (!(resp instanceof Response)) {
      throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    if (!resp.ok) backendError(resp);
    return resp.json();
  }

  /** `env.DB.query(sql, params?)` — single statement, Neon wire. */
  async query(sql: unknown, params?: unknown): Promise<unknown> {
    return this.#post({ query: boundSql(sql), params: boundParams(params) });
  }

  /** Neon parity alias. */
  async execute(sql: unknown, params?: unknown): Promise<unknown> {
    return this.query(sql, params);
  }

  /** `env.DB.transaction([{sql,params}…], opts?)` — atomic batch; opts
   * map to `neon-batch-*` headers (isolationLevel/readOnly/deferrable).
   * A read-only GRANT is enforced provider-side regardless of opts. */
  async transaction(
    statements: unknown,
    opts?: { isolationLevel?: string; readOnly?: boolean; deferrable?: boolean },
  ): Promise<unknown> {
    if (!Array.isArray(statements) || statements.length === 0 || statements.length > MAX_STATEMENTS) {
      throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    const queries = statements.map((s) => {
      if (typeof s !== "object" || s === null) throw bindingError("BINDING_PROTOCOL_ERROR");
      const stmt = s as Record<string, unknown>;
      return { query: boundSql(stmt.query ?? stmt.sql), params: boundParams(stmt.params) };
    });
    const headers: Record<string, string> = {};
    if (opts?.isolationLevel) headers["neon-batch-isolation-level"] = String(opts.isolationLevel);
    if (opts?.readOnly) headers["neon-batch-read-only"] = "true";
    if (opts?.deferrable) headers["neon-batch-deferrable"] = "true";
    const target = new URL(
      `${PG_PROVIDER_PREFIX}${this.#props.bindingId}/sql`,
      "http://pg.internal",
    ).href;
    const resp = await boundedFetch(this.#backend, target, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-open-compute-request-id": crypto.randomUUID(),
        ...headers,
      },
      body: JSON.stringify({ queries }),
    });
    if (!(resp instanceof Response)) throw bindingError("BINDING_PROTOCOL_ERROR");
    if (!resp.ok) backendError(resp);
    return resp.json();
  }

  /** fetch() IS the neon() fetchFunction entry: neon() issues POSTs
   * with a Neon-wire JSON body to its configured endpoint; we answer
   * the same contract against the trusted provider path. URL and any
   * inbound headers are ignored/stripped — only the parsed body is
   * forwarded. */
  async fetch(request: Request): Promise<Response> {
    if (request.method !== "POST") {
      return pgError("BINDING_PROTOCOL_ERROR");
    }
    let body: unknown;
    try {
      const text = await request.text();
      if (text.length > MAX_BODY_BYTES) return pgError("BINDING_LIMIT_EXCEEDED");
      body = JSON.parse(text);
    } catch {
      return pgError("BINDING_PROTOCOL_ERROR");
    }
    if (typeof body !== "object" || body === null) {
      return pgError("BINDING_PROTOCOL_ERROR");
    }
    const record = body as Record<string, unknown>;
    try {
      if (Array.isArray(record.queries)) {
        const queries = record.queries.map((q: Record<string, unknown>) => ({
          query: boundSql(q.query ?? q.sql),
          params: boundParams(q.params),
        }));
        return Response.json(await this.#post({ queries }));
      }
      return Response.json(
        await this.#post({ query: boundSql(record.query ?? record.sql), params: boundParams(record.params) }),
      );
    } catch (error) {
      const code =
        error instanceof Error && /^[A-Z_]+$/.test(error.message)
          ? error.message
          : "BINDING_PROTOCOL_ERROR";
      return pgError(code);
    }
  }

  /** TCP `connect()` remains unavailable (Hyperdrive-style compat is
   * T-future — see doc/platform/workers-runtime-pg-binding.md §12). */
  async connect(): Promise<never> {
    throw bindingError("BINDING_CAPABILITY_UNSUPPORTED");
  }
  async prepare(): Promise<never> {
    throw bindingError("BINDING_CAPABILITY_UNSUPPORTED");
  }
}
export default PgTransport;
