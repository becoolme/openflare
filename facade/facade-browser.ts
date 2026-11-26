// facade-browser system worker — the `browser` binding endpoint. Speaks
// CF's Browser Rendering `/v1/*` protocol (what `@cloudflare/puppeteer`
// sends to `env.BROWSER.fetch`) and bridges it onto the vendored
// kitesurf engine (`facade-kitesurf-front` → `facade-kitesurf`).
//
//   POST /v1/devtools/browser[?<params>]  → acquire → {sessionId}
//   GET  /v1/devtools/browser/<id>  (ws)  → attach + CDP pipe
//   GET  /v1/devtools/browser       (ws)  → direct connect + CDP pipe
//   GET  /v1/sessions | /v1/history | /v1/limits
//   GET  /json/version | /json/list       → engine discovery passthrough
//
// Session scoping: `resourceProps(ctx.props)` yields the host-injected
// tenant id; the registry is a module-scope Map keyed by it — a tenant
// can pass nothing but HTTP params, so sessions never cross tenants.
// WebSocket objects never cross requests (IoContext rule): the upstream
// engine socket is opened inside the SAME fetch that accepts the client
// socket, and the registry holds only plain-data records.

import { WorkerEntrypoint } from "cloudflare:workers";
import { resourceProps } from "./props.js";
import {
  SessionRegistry,
  browserLimits,
  newSessionId,
  sessionView,
  type BrowserLimits,
} from "./browser-core.js";

// One registry per tenant instance; the module persists for the isolate's
// life, so eviction/restart drops all registries — every open socket dies
// with them anyway (same blast radius as the sessions themselves).
const REGISTRIES = new Map<string, SessionRegistry>();

function registryFor(scope: string): SessionRegistry {
  let reg = REGISTRIES.get(scope);
  if (reg === undefined) {
    reg = new SessionRegistry();
    REGISTRIES.set(scope, reg);
  }
  return reg;
}

interface BrowserEnv {
  ENGINE: { fetch(req: Request | string, init?: RequestInit): Promise<Response> };
  BROWSER_LIMITS?: unknown;
}

const ENGINE_BASE = "http://kitesurf";

const text = (body: string, status: number): Response =>
  new Response(body, { status, headers: { "content-type": "text/plain" } });

const json = (value: unknown): Response =>
  new Response(JSON.stringify(value), {
    status: 200,
    headers: { "content-type": "application/json" },
  });

/** Pipe a client WebSocket to an upstream engine WebSocket 1:1 inside
 * this request's IoContext. On any close/error both ends die and the
 * session record transitions to closed. Returns the 101 upgrade. */
async function pipeToEngine(
  env: BrowserEnv,
  reg: SessionRegistry,
  sessionId: string,
): Promise<Response> {
  const upstream = await env.ENGINE.fetch(`${ENGINE_BASE}/devtools/browser`, {
    headers: { upgrade: "websocket" },
  });
  const engine = upstream.webSocket;
  if (upstream.status !== 101 || engine === null) {
    await upstream.body?.cancel().catch(() => {});
    reg.close(sessionId, "engine_connect_failed");
    return text("browser engine connect failed", 502);
  }
  engine.accept();

  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);
  server.accept();

  const deadline = () => reg.deadline(sessionId);
  const finish = (reason: string): void => {
    try {
      server.close(1000, reason);
    } catch {
      /* already closing */
    }
    try {
      engine.close(1000, reason);
    } catch {
      /* already closing */
    }
    reg.close(sessionId, reason);
  };
  const expired = (): boolean => {
    const d = deadline();
    return d !== 0 && Date.now() > d;
  };

  server.addEventListener("message", (event: MessageEvent) => {
    try {
      if (expired()) {
        finish("session_duration_limit");
        return;
      }
      engine.send(event.data);
    } catch {
      finish("pipe_error");
    }
  });
  engine.addEventListener("message", (event: MessageEvent) => {
    try {
      server.send(event.data);
    } catch {
      finish("pipe_error");
    }
  });
  server.addEventListener("close", () => finish("client_closed"));
  engine.addEventListener("close", () => finish("engine_closed"));
  server.addEventListener("error", () => finish("client_error"));
  engine.addEventListener("error", () => finish("engine_error"));

  return new Response(null, { status: 101, webSocket: client });
}

export class BrowserBinding extends WorkerEntrypoint<BrowserEnv> {
  async fetch(request: Request): Promise<Response> {
    // Host-injected tenant identity — a tenant supplies only HTTP input;
    // missing/malformed props fail closed before any work.
    const scope = resourceProps(this.ctx.props).instanceId;
    const reg = registryFor(scope);
    const limits = browserLimits((this.env as BrowserEnv).BROWSER_LIMITS);
    const url = new URL(request.url);
    const { pathname } = url;
    const wantsWs = request.headers.get("upgrade") === "websocket";

    // ── POST /v1/devtools/browser — acquire a session id. No engine
    // work happens here: the reservation is a registry entry the later
    // ws attach claims (the engine has no session without a socket).
    if (pathname === "/v1/devtools/browser" && request.method === "POST") {
      const engine = SessionRegistry.checkEngine(url.searchParams.get("browser"));
      if (typeof engine !== "string") {
        return text(engine.error, 400);
      }
      const ka = SessionRegistry.keepAliveMs(
        url.searchParams.get("keep_alive"),
        limits,
      );
      if (!ka.ok) return text(ka.detail, 400);
      const res = reg.acquire(engine, ka.ms, limits, newSessionId);
      if (!res.ok) {
        return text(res.detail, res.error === "concurrency_limit" ? 429 : 400);
      }
      return json({ sessionId: res.sessionId });
    }

    // ── GET /v1/devtools/browser[/<sessionId>] — ws attach / direct.
    if (pathname === "/v1/devtools/browser" && wantsWs) {
      const engine = SessionRegistry.checkEngine(url.searchParams.get("browser"));
      if (typeof engine !== "string") {
        return text(engine.error, 400);
      }
      const res = reg.attach(undefined, engine, limits, newSessionId);
      if (!res.ok) return text(res.error, res.status);
      return pipeToEngine(this.env as BrowserEnv, reg, res.rec.sessionId);
    }
    const attachMatch = /^\/v1\/devtools\/browser\/([0-9a-f]{32})$/.exec(pathname);
    if (attachMatch !== null) {
      if (!wantsWs) return text("websocket upgrade required", 400);
      const res = reg.attach(attachMatch[1], "kitesurf", limits, newSessionId);
      if (!res.ok) return text(res.error, res.status);
      return pipeToEngine(this.env as BrowserEnv, reg, res.rec.sessionId);
    }

    // ── GET /v1/sessions | /v1/history | /v1/limits — registry reads.
    if (pathname === "/v1/sessions") {
      return json({ sessions: reg.sessions().map(sessionView) });
    }
    if (pathname === "/v1/history") {
      return json({ history: reg.history(limits.historyCap).map(sessionView) });
    }
    if (pathname === "/v1/limits") {
      return json(reg.counters(limits));
    }

    // ── /json/* — engine discovery passthrough (super.connect() fallback
    // and devtools tooling read webSocketDebuggerUrl here).
    if (
      pathname === "/json/version" ||
      pathname === "/json/version/" ||
      pathname === "/json" ||
      pathname === "/json/list" ||
      pathname === "/json/list/" ||
      pathname === "/json/protocol" ||
      pathname === "/health"
    ) {
      return await (this.env as BrowserEnv).ENGINE.fetch(request);
    }

    return text("not found", 404);
  }
}

export default BrowserBinding;
