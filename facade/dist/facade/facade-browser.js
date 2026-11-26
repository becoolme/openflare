import { WorkerEntrypoint } from "cloudflare:workers";
import { resourceProps } from "./props.js";
import { SessionRegistry, browserLimits, newSessionId, sessionView } from "./browser-core.js";
const REGISTRIES = new Map();
function registryFor(scope) {
    let reg = REGISTRIES.get(scope);
    if (reg === undefined) {
        reg = new SessionRegistry();
        REGISTRIES.set(scope, reg);
    }
    return reg;
}
const ENGINE_BASE = "http://kitesurf";
const text = (body, status)=>new Response(body, {
        status,
        headers: {
            "content-type": "text/plain"
        }
    });
const json = (value)=>new Response(JSON.stringify(value), {
        status: 200,
        headers: {
            "content-type": "application/json"
        }
    });
async function pipeToEngine(env, reg, sessionId) {
    const upstream = await env.ENGINE.fetch(`${ENGINE_BASE}/devtools/browser`, {
        headers: {
            upgrade: "websocket"
        }
    });
    const engine = upstream.webSocket;
    if (upstream.status !== 101 || engine === null) {
        await upstream.body?.cancel().catch(()=>{});
        reg.close(sessionId, "engine_connect_failed");
        return text("browser engine connect failed", 502);
    }
    engine.accept();
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    const deadline = ()=>reg.deadline(sessionId);
    const finish = (reason)=>{
        try {
            server.close(1000, reason);
        } catch  {}
        try {
            engine.close(1000, reason);
        } catch  {}
        reg.close(sessionId, reason);
    };
    const expired = ()=>{
        const d = deadline();
        return d !== 0 && Date.now() > d;
    };
    server.addEventListener("message", (event)=>{
        try {
            if (expired()) {
                finish("session_duration_limit");
                return;
            }
            engine.send(event.data);
        } catch  {
            finish("pipe_error");
        }
    });
    engine.addEventListener("message", (event)=>{
        try {
            server.send(event.data);
        } catch  {
            finish("pipe_error");
        }
    });
    server.addEventListener("close", ()=>finish("client_closed"));
    engine.addEventListener("close", ()=>finish("engine_closed"));
    server.addEventListener("error", ()=>finish("client_error"));
    engine.addEventListener("error", ()=>finish("engine_error"));
    return new Response(null, {
        status: 101,
        webSocket: client
    });
}
export class BrowserBinding extends WorkerEntrypoint {
    async fetch(request) {
        const scope = resourceProps(this.ctx.props).instanceId;
        const reg = registryFor(scope);
        const limits = browserLimits(this.env.BROWSER_LIMITS);
        const url = new URL(request.url);
        const { pathname } = url;
        const wantsWs = request.headers.get("upgrade") === "websocket";
        if (pathname === "/v1/devtools/browser" && request.method === "POST") {
            const engine = SessionRegistry.checkEngine(url.searchParams.get("browser"));
            if (typeof engine !== "string") {
                return text(engine.error, 400);
            }
            const ka = SessionRegistry.keepAliveMs(url.searchParams.get("keep_alive"), limits);
            if (!ka.ok) return text(ka.detail, 400);
            const res = reg.acquire(engine, ka.ms, limits, newSessionId);
            if (!res.ok) {
                return text(res.detail, res.error === "concurrency_limit" ? 429 : 400);
            }
            return json({
                sessionId: res.sessionId
            });
        }
        if (pathname === "/v1/devtools/browser" && wantsWs) {
            const engine = SessionRegistry.checkEngine(url.searchParams.get("browser"));
            if (typeof engine !== "string") {
                return text(engine.error, 400);
            }
            const res = reg.attach(undefined, engine, limits, newSessionId);
            if (!res.ok) return text(res.error, res.status);
            return pipeToEngine(this.env, reg, res.rec.sessionId);
        }
        const attachMatch = /^\/v1\/devtools\/browser\/([0-9a-f]{32})$/.exec(pathname);
        if (attachMatch !== null) {
            if (!wantsWs) return text("websocket upgrade required", 400);
            const res = reg.attach(attachMatch[1], "kitesurf", limits, newSessionId);
            if (!res.ok) return text(res.error, res.status);
            return pipeToEngine(this.env, reg, res.rec.sessionId);
        }
        if (pathname === "/v1/sessions") {
            return json({
                sessions: reg.sessions().map(sessionView)
            });
        }
        if (pathname === "/v1/history") {
            return json({
                history: reg.history(limits.historyCap).map(sessionView)
            });
        }
        if (pathname === "/v1/limits") {
            return json(reg.counters(limits));
        }
        if (pathname === "/json/version" || pathname === "/json/version/" || pathname === "/json" || pathname === "/json/list" || pathname === "/json/list/" || pathname === "/json/protocol" || pathname === "/health") {
            return await this.env.ENGINE.fetch(request);
        }
        return text("not found", 404);
    }
}
export default BrowserBinding;
