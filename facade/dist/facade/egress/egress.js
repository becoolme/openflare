import { WorkerEntrypoint } from "cloudflare:workers";
import { connect as upstreamConnect } from "cloudflare:sockets";
import { evaluateTarget, evaluateTcpTarget, parseEgressPolicy } from "./policy.js";
import { classifyUpstreamError, closeQuietly, DENIED_HEADER, ERROR_HEADER, logTcpClose, logTcpDeny, logUpstreamError, POLICY_BINDING, stripInternalHeaders, tcpDenied, upstreamFetchError } from "./shared.js";
import { cfImageBody, cfImagePlan, SRC_MAX } from "./cf-image.js";
import { sniffImage, sniffSvg } from "../images-wire.js";
import { hushFilter, HushError } from "../svg-hush.js";
function denied(code, detail) {
    const body = JSON.stringify({
        error: "EGRESS_DENIED",
        code,
        ...detail === undefined ? {} : {
            detail
        }
    });
    return new Response(body, {
        status: 403,
        headers: {
            "content-type": "application/json",
            "cache-control": "no-store",
            [DENIED_HEADER]: code
        }
    });
}
function upstreamError() {
    return new Response(JSON.stringify({
        error: "EGRESS_UPSTREAM"
    }), {
        status: 502,
        headers: {
            "content-type": "application/json",
            "cache-control": "no-store",
            [ERROR_HEADER]: "upstream"
        }
    });
}
const activeTcpSessions = new Set();
const REAP_GRACE_MS = 1_000;
function reapAbandonedTcp(now) {
    for (const sess of activeTcpSessions){
        if (now - sess.lastActivity > sess.silenceBudgetMs + REAP_GRACE_MS) {
            activeTcpSessions.delete(sess);
            logTcpClose(sess.port, sess.counters.up, sess.counters.down, sess.lastActivity - sess.started, "abandoned");
        }
    }
}
const OPENED_WAIT_MS = 5_000;
async function connectAuthority(s) {
    if (s === undefined || s === null || s.opened === undefined) return null;
    try {
        const info = await Promise.race([
            s.opened,
            sleep(OPENED_WAIT_MS).then(()=>null)
        ]);
        return info?.localAddress ?? null;
    } catch  {
        return null;
    }
}
const INTERNAL_TCP_PREAMBLES = {
    "pg-v1": (host)=>new TextEncoder().encode(`VRT-PG1 ${host.split(".")[0] ?? ""}\n`)
};
const sleep = (ms)=>new Promise((resolve)=>setTimeout(resolve, ms));
async function pumpTcp(inbound, upstream, idleMs, counters, session) {
    let lastActivity = Date.now();
    session.lastActivity = lastActivity;
    session.silenceBudgetMs = idleMs;
    const meter = (dir)=>new TransformStream({
            transform (chunk, controller) {
                counters[dir] += chunk?.byteLength ?? 0;
                lastActivity = Date.now();
                session.lastActivity = lastActivity;
                controller.enqueue(chunk);
            }
        });
    const upDone = inbound.readable.pipeThrough(meter("up")).pipeTo(upstream.writable).then(()=>"up_end", ()=>"up_err");
    const downDone = upstream.readable.pipeThrough(meter("down")).pipeTo(inbound.writable).then(()=>"down_end", ()=>"down_err");
    let finished = false;
    let finishNow = ()=>{};
    const finishSignal = new Promise((resolve)=>{
        finishNow = resolve;
    });
    const tick = Math.max(50, Math.min(500, Math.floor(idleMs / 4)));
    const idleWatch = (async ()=>{
        while(!finished){
            await Promise.race([
                sleep(tick),
                finishSignal
            ]);
            if (finished) return "watch_end";
            if (Date.now() - lastActivity >= idleMs) return "idle";
        }
        return "watch_end";
    })();
    let reason;
    const first = await Promise.race([
        downDone,
        upDone,
        idleWatch
    ]);
    if (first === "idle") {
        reason = "idle_timeout";
    } else if (first === "down_end") {
        reason = "eof";
    } else if (first === "up_end") {
        const second = await Promise.race([
            downDone,
            idleWatch
        ]);
        reason = second === "idle" ? "idle_timeout" : second === "down_err" ? "error" : "eof";
    } else {
        reason = "error";
    }
    finished = true;
    finishNow();
    await closeQuietly(upstream);
    await closeQuietly(inbound);
    await Promise.allSettled([
        upDone,
        downDone,
        idleWatch
    ]);
    return reason;
}
export class EgressProxy extends WorkerEntrypoint {
    #policy;
    #env;
    constructor(ctx, env){
        super(ctx, env);
        this.#env = env;
        const raw = env?.[POLICY_BINDING];
        this.#policy = parseEgressPolicy(raw);
    }
    async fetch(request) {
        const decision = evaluateTarget(request.url, this.#policy);
        if (!decision.allow) return denied(decision.code, decision.detail);
        let upstream;
        const started = Date.now();
        try {
            upstream = await fetch(request, {
                headers: stripInternalHeaders(request.headers),
                redirect: "manual",
                signal: AbortSignal.timeout(this.#policy.requestTimeoutMs)
            });
        } catch (e) {
            const cls = classifyUpstreamError(e);
            logUpstreamError(cls, request.url, Date.now() - started, e);
            throw upstreamFetchError(cls);
        }
        if (upstream.status === 101) {
            const webSocket = upstream.webSocket;
            if (webSocket === undefined || webSocket === null) {
                return upstreamError();
            }
            try {
                return new Response(null, {
                    status: 101,
                    headers: stripInternalHeaders(upstream.headers),
                    webSocket
                });
            } catch  {
                return upstreamError();
            }
        }
        const plan = cfImagePlan(request);
        if (plan !== undefined) {
            return this.#transformImage(request, upstream, plan);
        }
        return new Response(upstream.body, {
            status: upstream.status,
            statusText: upstream.statusText,
            headers: stripInternalHeaders(upstream.headers)
        });
    }
    async #transformImage(request, upstream, plan) {
        const url = new URL(request.url);
        const redirect = ()=>new Response(null, {
                status: 302,
                headers: {
                    location: url.toString()
                }
            });
        const svc = this.#env?.["VRT_IMAGES"];
        if (svc === undefined || typeof svc.fetch !== "function") {
            return new Response(`ERROR 9490\n`, {
                status: 500,
                headers: {
                    "content-type": "text/plain",
                    "cf-resized": "err=9490"
                }
            });
        }
        let out;
        try {
            const src = new Uint8Array(await upstream.arrayBuffer());
            if (src.length > SRC_MAX) {
                return new Response("ERROR 9424\n", {
                    status: 413,
                    headers: {
                        "content-type": "text/plain",
                        "cf-resized": "err=9424"
                    }
                });
            }
            if (sniffSvg(src)) {
                try {
                    return new Response(hushFilter(src), {
                        headers: {
                            "content-type": "image/svg+xml"
                        }
                    });
                } catch (e) {
                    if (!(e instanceof HushError)) throw e;
                    if (plan.onerrorRedirect) return redirect();
                    return new Response("ERROR 9420\n", {
                        status: 415,
                        headers: {
                            "content-type": "text/plain",
                            "cf-resized": "err=9420"
                        }
                    });
                }
            }
            out = await svc.fetch("http://vrt-images.internal/transform", {
                method: "POST",
                body: cfImageBody(src, plan)
            });
        } catch  {
            if (plan.onerrorRedirect) return redirect();
            return new Response("ERROR 9523\n", {
                status: 500,
                headers: {
                    "content-type": "text/plain",
                    "cf-resized": "err=9523"
                }
            });
        }
        const err = out.headers.get("cf-images-binding");
        if (err !== null) {
            const code = /^err=\d+$/.test(err) ? err.slice(4) : "9523";
            if (plan.onerrorRedirect) return redirect();
            return new Response(`ERROR ${code}\n`, {
                status: out.status,
                headers: {
                    "content-type": "text/plain",
                    "cf-resized": `err=${code}`
                }
            });
        }
        if (plan.json) {
            const bytes = new Uint8Array(await out.arrayBuffer());
            const info = sniffImage(bytes);
            return new Response(JSON.stringify({
                width: info?.width ?? 0,
                height: info?.height ?? 0,
                format: info ? `image/${info.format}` : "unknown",
                size: bytes.length
            }), {
                headers: {
                    "content-type": "application/json"
                }
            });
        }
        const headers = stripInternalHeaders(out.headers);
        headers.delete("content-length");
        return new Response(out.body, {
            status: out.status,
            headers
        });
    }
    async connect(socket) {
        const s = socket;
        reapAbandonedTcp(Date.now());
        const decision = evaluateTcpTarget(await connectAuthority(s), this.#policy);
        if (!decision.allow) {
            logTcpDeny(decision.port, decision.code);
            await closeQuietly(s);
            throw tcpDenied(decision.code, decision.detail);
        }
        const policy = this.#policy;
        const target = decision.target;
        const internal = decision.internal;
        if (activeTcpSessions.size >= policy.tcpMaxConcurrent) {
            logTcpDeny(target.port, "concurrent_limit");
            await closeQuietly(s);
            throw tcpDenied("concurrent_limit");
        }
        const started = Date.now();
        const counters = {
            up: 0,
            down: 0
        };
        const session = {
            port: target.port,
            started,
            lastActivity: started,
            silenceBudgetMs: policy.tcpConnectTimeoutMs,
            counters
        };
        activeTcpSessions.add(session);
        let upstream = null;
        let reason = "eof";
        try {
            try {
                if (internal !== undefined) {
                    const svc = this.#env?.["INTERNAL_NET"];
                    if (svc === undefined || typeof svc.connect !== "function") {
                        reason = "config";
                        throw tcpDenied("config");
                    }
                    upstream = svc.connect({
                        hostname: internal.upstreamHost,
                        port: internal.upstreamPort
                    });
                } else {
                    upstream = upstreamConnect({
                        hostname: target.dialHost,
                        port: target.port
                    });
                }
            } catch (e) {
                if (reason === "config") throw e;
                const cls = classifyUpstreamError(e);
                reason = cls;
                throw upstreamFetchError(cls);
            }
            let dialError;
            const opened = await Promise.race([
                upstream.opened.then(()=>"opened", (e)=>{
                    dialError = e;
                    return "failed";
                }),
                sleep(policy.tcpConnectTimeoutMs).then(()=>"timeout")
            ]);
            if (opened === "timeout") {
                reason = "connect_timeout";
                throw tcpDenied("connect_timeout");
            }
            if (opened === "failed") {
                const cls = classifyUpstreamError(dialError);
                reason = cls;
                throw upstreamFetchError(cls);
            }
            if (internal !== undefined && upstream !== null) {
                const encode = INTERNAL_TCP_PREAMBLES[internal.kind];
                if (encode === undefined) throw tcpDenied("config");
                try {
                    const w = upstream.writable.getWriter();
                    await w.write(encode(target.host));
                    w.releaseLock();
                } catch (e) {
                    const cls = classifyUpstreamError(e);
                    reason = cls;
                    throw upstreamFetchError(cls);
                }
            }
            reason = await pumpTcp(s, upstream, policy.tcpIdleTimeoutMs, counters, session);
            if (reason === "idle_timeout") throw tcpDenied("idle_timeout");
        } finally{
            const ours = activeTcpSessions.delete(session);
            if (upstream !== null) await closeQuietly(upstream);
            await closeQuietly(s);
            if (ours) {
                logTcpClose(target.port, counters.up, counters.down, Date.now() - started, reason);
            }
        }
    }
}
export default EgressProxy;
