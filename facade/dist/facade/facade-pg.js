import { WorkerEntrypoint } from "cloudflare:workers";
import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";
import { BACKEND_TIMEOUT, PG_BACKEND_TIMEOUT_MS, backendCallWithTimeout } from "./deadline.js";
import { resourceProps, wrapBindingBackend } from "./props.js";
const PG_PROVIDER_PREFIX = "/internal/bindings/v1/pg/";
const OPEN_COMPUTE_PREFIX = "x-open-compute-";
const VRT_PREFIX = "x-vrt-";
const MAX_BODY_BYTES = 256 * 1024;
const MAX_SQL_CHARS = 64 * 1024;
const MAX_PARAMS = 255;
const MAX_STATEMENTS = 64;
const MAX_PARAM_DEPTH = 16;
function pgError(code, retryable = false, resultUnknown = false) {
    return new Response(JSON.stringify({
        ok: false,
        error: {
            code,
            retryable,
            resultUnknown
        }
    }), {
        status: 422,
        headers: {
            "content-type": "application/json",
            "x-open-compute-error-code": code
        }
    });
}
async function boundedFetch(backend, target, init) {
    const resp = await backendCallWithTimeout(backend, target, init, PG_BACKEND_TIMEOUT_MS);
    if (resp === BACKEND_TIMEOUT) throw bindingError("BINDING_BACKEND_TIMEOUT");
    if (resp instanceof Error) throw bindingError("BINDING_BACKEND_UNAVAILABLE");
    return resp;
}
function backendError(resp) {
    const code = resp.headers.get("x-open-compute-error-code") ?? "BINDING_PROTOCOL_ERROR";
    throw bindingError(code);
}
function boundValue(value, depth) {
    if (depth > MAX_PARAM_DEPTH) throw bindingError("BINDING_LIMIT_EXCEEDED");
    if (Array.isArray(value)) {
        return value.map((v)=>boundValue(v, depth + 1));
    }
    if (value !== null && typeof value === "object") {
        if (Object.getPrototypeOf(value) !== Object.prototype) {
            throw bindingError("BINDING_PROTOCOL_ERROR");
        }
        const out = {};
        for (const [k, v] of Object.entries(value))out[k] = boundValue(v, depth + 1);
        return out;
    }
    if (typeof value === "string" && value.length > MAX_BODY_BYTES) {
        throw bindingError("BINDING_LIMIT_EXCEEDED");
    }
    return value;
}
function boundParams(params) {
    if (params === undefined || params === null) return [];
    if (!Array.isArray(params)) throw bindingError("BINDING_PROTOCOL_ERROR");
    if (params.length > MAX_PARAMS) throw bindingError("BINDING_LIMIT_EXCEEDED");
    return params.map((p)=>boundValue(p, 0));
}
function boundSql(sql) {
    if (typeof sql !== "string" || sql.length === 0 || sql.length > MAX_SQL_CHARS) {
        throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    return sql;
}
export class PgTransport extends WorkerEntrypoint {
    #props;
    #backend;
    constructor(ctx, env){
        super(ctx, env);
        this.#props = resourceProps(ctx?.props);
        this.#backend = wrapBindingBackend(env?.BINDING_BACKEND, ctx?.props);
    }
    async #post(body) {
        const target = new URL(`${PG_PROVIDER_PREFIX}${this.#props.bindingId}/sql`, "http://pg.internal").href;
        const resp = await boundedFetch(this.#backend, target, {
            method: "POST",
            headers: {
                "content-type": "application/json",
                "x-open-compute-request-id": crypto.randomUUID()
            },
            body: JSON.stringify(body)
        });
        if (!(resp instanceof Response)) {
            throw bindingError("BINDING_PROTOCOL_ERROR");
        }
        if (!resp.ok) backendError(resp);
        return resp.json();
    }
    async query(sql, params) {
        return this.#post({
            query: boundSql(sql),
            params: boundParams(params)
        });
    }
    async execute(sql, params) {
        return this.query(sql, params);
    }
    async transaction(statements, opts) {
        if (!Array.isArray(statements) || statements.length === 0 || statements.length > MAX_STATEMENTS) {
            throw bindingError("BINDING_PROTOCOL_ERROR");
        }
        const queries = statements.map((s)=>{
            if (typeof s !== "object" || s === null) throw bindingError("BINDING_PROTOCOL_ERROR");
            const stmt = s;
            return {
                query: boundSql(stmt.query ?? stmt.sql),
                params: boundParams(stmt.params)
            };
        });
        const headers = {};
        if (opts?.isolationLevel) headers["neon-batch-isolation-level"] = String(opts.isolationLevel);
        if (opts?.readOnly) headers["neon-batch-read-only"] = "true";
        if (opts?.deferrable) headers["neon-batch-deferrable"] = "true";
        const target = new URL(`${PG_PROVIDER_PREFIX}${this.#props.bindingId}/sql`, "http://pg.internal").href;
        const resp = await boundedFetch(this.#backend, target, {
            method: "POST",
            headers: {
                "content-type": "application/json",
                "x-open-compute-request-id": crypto.randomUUID(),
                ...headers
            },
            body: JSON.stringify({
                queries
            })
        });
        if (!(resp instanceof Response)) throw bindingError("BINDING_PROTOCOL_ERROR");
        if (!resp.ok) backendError(resp);
        return resp.json();
    }
    async fetch(request) {
        if (request.method !== "POST") {
            return pgError("BINDING_PROTOCOL_ERROR");
        }
        let body;
        try {
            const text = await request.text();
            if (text.length > MAX_BODY_BYTES) return pgError("BINDING_LIMIT_EXCEEDED");
            body = JSON.parse(text);
        } catch  {
            return pgError("BINDING_PROTOCOL_ERROR");
        }
        if (typeof body !== "object" || body === null) {
            return pgError("BINDING_PROTOCOL_ERROR");
        }
        const record = body;
        try {
            if (Array.isArray(record.queries)) {
                const queries = record.queries.map((q)=>({
                        query: boundSql(q.query ?? q.sql),
                        params: boundParams(q.params)
                    }));
                return Response.json(await this.#post({
                    queries
                }));
            }
            return Response.json(await this.#post({
                query: boundSql(record.query ?? record.sql),
                params: boundParams(record.params)
            }));
        } catch (error) {
            const code = error instanceof Error && /^[A-Z_]+$/.test(error.message) ? error.message : "BINDING_PROTOCOL_ERROR";
            return pgError(code);
        }
    }
    async connect() {
        throw bindingError("BINDING_CAPABILITY_UNSUPPORTED");
    }
    async prepare() {
        throw bindingError("BINDING_CAPABILITY_UNSUPPORTED");
    }
}
export default PgTransport;
