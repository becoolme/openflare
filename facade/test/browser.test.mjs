import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule, makeProps } from "./helpers.mjs";

const core = await import(facadeModule("browser-core.js"));
const { BrowserBinding } = await import(facadeModule("facade-browser.js"));
const {
  SessionRegistry,
  browserLimits,
  newSessionId,
  sessionView,
  DEFAULT_LIMITS,
} = core;

let nextId = 0;
// Distinct ids per mint — sessionIds are Map keys, so a constant factory
// would collapse "two sessions" into one record.
const ID = () => `${++nextId}`.padStart(32, "0");

// ── limits validation ──────────────────────────────────────────────

test("limits: defaults, overrides, and fail-closed shapes", () => {
  assert.deepEqual(browserLimits(undefined), DEFAULT_LIMITS);
  assert.deepEqual(browserLimits(null), DEFAULT_LIMITS);
  assert.equal(browserLimits({ maxConcurrentSessions: 2 }).maxConcurrentSessions, 2);
  for (const bad of [
    "x", [], { maxConcurrentSessions: 0 }, { maxConcurrentSessions: 65 },
    { maxSessionMs: "x" }, { historyCap: -1 }, { pendingTtlMs: 999 },
    { maxKeepAliveMs: 3_600_001 },
  ]) {
    assert.throws(() => browserLimits(bad), String(bad));
  }
});

// ── engine + param parsing ─────────────────────────────────────────

test("engine selection: absent/kitesurf pass, others fail closed", () => {
  for (const ok of [null, "", "kitesurf"]) {
    assert.equal(SessionRegistry.checkEngine(ok), "kitesurf");
  }
  for (const bad of ["chromium", "KITESURF", "kitesurf%00"]) {
    const r = SessionRegistry.checkEngine(bad);
    assert.equal(typeof r, "object");
  }
});

test("keep_alive: absent → pendingTtl; integer clamps to max; junk fails", () => {
  const lim = { ...DEFAULT_LIMITS, pendingTtlMs: 5_000, maxKeepAliveMs: 10_000 };
  assert.deepEqual(SessionRegistry.keepAliveMs(null, lim), { ok: true, ms: 5_000 });
  assert.deepEqual(SessionRegistry.keepAliveMs("1000", lim), { ok: true, ms: 1_000 });
  assert.deepEqual(SessionRegistry.keepAliveMs("99999", lim), { ok: true, ms: 10_000 });
  for (const bad of ["x", "-1", "1.5", ""]) {
    assert.equal(SessionRegistry.keepAliveMs(bad, lim).ok, false);
  }
});

// ── registry transitions ───────────────────────────────────────────

test("acquire → attach → close lifecycle; counters track", () => {
  let now = 1_000;
  const reg = new SessionRegistry(() => now);
  const lim = { ...DEFAULT_LIMITS, maxSessionMs: 60_000 };
  const a = reg.acquire("kitesurf", 30_000, lim, ID);
  assert.equal(a.ok, true);
  let res = reg.attach(a.sessionId, "kitesurf", lim, ID);
  assert.equal(res.ok, true);
  assert.equal(res.rec.state, "active");
  assert.equal(res.rec.deadline, now + 60_000);
  // Re-attach to an active session fails.
  res = reg.attach(a.sessionId, "kitesurf", lim, ID);
  assert.equal(res.status, 409);
  assert.deepEqual(reg.counters(lim).activeSessions, 1);
  reg.close(a.sessionId, "client_closed");
  assert.deepEqual(reg.counters(lim).activeSessions, 0);
  res = reg.attach(a.sessionId, "kitesurf", lim, ID);
  assert.equal(res.status, 410);
});

test("unknown sessionId → 404", () => {
  const reg = new SessionRegistry();
  const res = reg.attach("f".repeat(32), "kitesurf", DEFAULT_LIMITS, ID);
  assert.equal(res.status, 404);
});

test("concurrency limit counts pending + active, sweeps expired pendings", () => {
  let now = 0;
  const reg = new SessionRegistry(() => now);
  const lim = { ...DEFAULT_LIMITS, maxConcurrentSessions: 2 };
  assert.equal(reg.acquire("kitesurf", 1_000, lim, ID).ok, true);
  assert.equal(reg.acquire("kitesurf", 1_000, lim, ID).ok, true);
  const third = reg.acquire("kitesurf", 1_000, lim, ID);
  assert.equal(third.ok, false);
  assert.equal(third.error, "concurrency_limit");
  assert.deepEqual(reg.counters(lim).allowedBrowserAcquisitions, 0);
  // Both pendings expire → sweeps free the slots.
  now = 2_000;
  assert.equal(reg.acquire("kitesurf", 1_000, lim, ID).ok, true);
});

test("pending expiry lands in history, not sessions", () => {
  let now = 0;
  const reg = new SessionRegistry(() => now);
  const lim = { ...DEFAULT_LIMITS, pendingTtlMs: 500, historyCap: 10 };
  const a = reg.acquire("kitesurf", 500, lim, ID);
  assert.equal(reg.sessions().length, 1);
  now = 600;
  assert.equal(reg.sessions().length, 0);
  const hist = reg.history(10);
  assert.equal(hist.length, 1);
  assert.equal(hist[0].sessionId, a.sessionId);
  assert.equal(hist[0].closeReason, "acquire_ttl_expired");
  // Known-but-gone → 410 (the id may still sit in a reconnect loop).
  assert.equal(reg.attach(a.sessionId, "kitesurf", lim, ID).status, 410);
});

test("direct attach mints an already-active session", () => {
  const reg = new SessionRegistry();
  const res = reg.attach(undefined, "kitesurf", DEFAULT_LIMITS, ID);
  assert.equal(res.ok, true);
  assert.equal(res.rec.state, "active");
  assert.equal(reg.attach(res.rec.sessionId, "kitesurf", DEFAULT_LIMITS, ID).status, 409);
});

test("sessionView: connectionId only while active; closed carries end", () => {
  const rec = {
    sessionId: "s", engine: "kitesurf", state: "pending",
    createdAt: 10, pendingUntil: 20,
  };
  assert.deepEqual(sessionView(rec), {
    sessionId: "s", startTime: 10, status: "pending",
  });
  rec.state = "active";
  assert.equal(sessionView(rec).connectionId, "s");
  rec.state = "closed"; rec.closedAt = 30; rec.closeReason = "x";
  const v = sessionView(rec);
  assert.equal(v.status, "closed");
  assert.equal(v.endTime, 30);
  assert.equal(v.reason, "x");
});

test("history bounds and orders newest-first", () => {
  let now = 0;
  const reg = new SessionRegistry(() => now);
  const lim = DEFAULT_LIMITS;
  for (let i = 0; i < 5; i++) {
    const a = reg.acquire("kitesurf", 1e9, lim, () => `${i}`.padStart(32, "0"));
    now += 1;
    reg.close(a.sessionId, "t");
  }
  const hist = reg.history(3);
  assert.equal(hist.length, 3);
  assert.ok(hist[0].closedAt > hist[1].closedAt);
});

test("newSessionId: 32 lowercase hex, unique", () => {
  const ids = new Set(Array.from({ length: 100 }, newSessionId));
  assert.equal(ids.size, 100);
  assert.match([...ids][0], /^[0-9a-f]{32}$/);
});

// ── entrypoint wire surface ────────────────────────────────────────

let nextScope = 0;
// REGISTRIES is isolate-global — tests must not leak sessions between
// each other through the shared tenant scope.
const binding = (env) =>
  new BrowserBinding(
    { props: makeProps({ instanceId: `tenant-t${++nextScope}` }) },
    env ?? { ENGINE: { fetch: async () => new Response("x") } },
  );

test("acquire endpoint: POST → {sessionId}; then it lists + counts", async () => {
  const b = binding();
  const res = await b.fetch(new Request("https://b/v1/devtools/browser", { method: "POST" }));
  assert.equal(res.status, 200);
  const { sessionId } = await res.json();
  assert.match(sessionId, /^[0-9a-f]{32}$/);
  const s = await (await b.fetch(new Request("https://b/v1/sessions"))).json();
  assert.equal(s.sessions.length, 1);
  assert.equal(s.sessions[0].sessionId, sessionId);
  const lim = await (await b.fetch(new Request("https://b/v1/limits"))).json();
  assert.equal(lim.activeSessions, 1);
  assert.equal(lim.maxConcurrentSessions, 4);
  assert.equal(lim.allowedBrowserAcquisitions, 3);
});

test("acquire rejects non-kitesurf engines and bad keep_alive", async () => {
  const b = binding();
  for (const url of [
    "https://b/v1/devtools/browser?browser=chromium",
    "https://b/v1/devtools/browser?keep_alive=x",
    "https://b/v1/devtools/browser?keep_alive=-5",
  ]) {
    const res = await b.fetch(new Request(url, { method: "POST" }));
    assert.equal(res.status, 400, url);
  }
});

test("attach without ws upgrade → 400; unknown → 404", async () => {
  const b = binding();
  const res = await b.fetch(
    new Request("https://b/v1/devtools/browser/" + "a".repeat(32)),
  );
  assert.equal(res.status, 400);
  const res2 = await b.fetch(
    new Request("https://b/v1/devtools/browser/" + "a".repeat(32), {
      headers: { upgrade: "websocket" },
    }),
  );
  assert.equal(res2.status, 404);
});

test("sessions/history isolated per tenant scope", async () => {
  const env = { ENGINE: { fetch: async () => new Response("x") } };
  const t1 = new BrowserBinding({ props: makeProps() }, env);
  const t2 = new BrowserBinding(
    { props: makeProps({ instanceId: "tenant-8" }) },
    env,
  );
  await t1.fetch(new Request("https://b/v1/devtools/browser", { method: "POST" }));
  const s2 = await (await t2.fetch(new Request("https://b/v1/sessions"))).json();
  assert.equal(s2.sessions.length, 0);
  const h2 = await (await t2.fetch(new Request("https://b/v1/history"))).json();
  assert.equal(h2.history.length, 0);
});

test("props fail closed — nothing a tenant sends can mint a scope", async () => {
  for (const props of [undefined, {}, { instanceId: "x" }, { bad: true }]) {
    const b = new BrowserBinding(
      { props },
      { ENGINE: { fetch: async () => new Response("x") } },
    );
    await assert.rejects(
      () => b.fetch(new Request("https://b/v1/limits")),
      props === undefined ? undefined : undefined,
    );
  }
});

test("engine unreachable at attach → 502 + session closes", async () => {
  const env = { ENGINE: { fetch: async () => new Response("nope", { status: 500 }) } };
  const b = binding(env);
  const acq = await b.fetch(new Request("https://b/v1/devtools/browser", { method: "POST" }));
  const { sessionId } = await acq.json();
  const res = await b.fetch(
    new Request(`https://b/v1/devtools/browser/${sessionId}`, {
      headers: { upgrade: "websocket" },
    }),
  );
  assert.equal(res.status, 502);
  const s = await (await b.fetch(new Request("https://b/v1/sessions"))).json();
  assert.equal(s.sessions.length, 0);
  const h = await (await b.fetch(new Request("https://b/v1/history"))).json();
  assert.equal(h.history[0].reason, "engine_connect_failed");
});

test("/json/* proxies to ENGINE; unknown paths 404", async () => {
  const seen = [];
  const env = {
    ENGINE: {
      fetch: async (req) => {
        seen.push(new URL(req.url ?? req).pathname);
        return new Response('{"Browser":"kitesurf"}', {
          headers: { "content-type": "application/json" },
        });
      },
    },
  };
  const b = binding(env);
  const res = await b.fetch(new Request("https://b/json/version"));
  assert.equal(res.status, 200);
  assert.deepEqual(seen, ["/json/version"]);
  assert.equal((await b.fetch(new Request("https://b/nope"))).status, 404);
});
