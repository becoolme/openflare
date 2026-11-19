import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { makeProps, makeBackend, facadeModule } from "./helpers.mjs";

const { D1Facade } = await import(facadeModule("facade-d1.js"));

function makeFacade(propsOverrides = {}, handler = null) {
  const backend = makeBackend(
    handler ?? (async () => new Response('{"ok":true}', {
      headers: { "content-type": "application/json" },
    })),
  );
  const env = { BINDING_BACKEND: backend.fetcher };
  const facade = new D1Facade({ props: makeProps(propsOverrides) }, env);
  return { facade, backend };
}

test("forwards /query preserving resultsFormat, body and session headers", async () => {
  let seen;
  const { facade } = makeFacade({}, async (record) => {
    seen = record;
    return new Response("[]", { headers: { "content-type": "application/json" } });
  });
  const req = new Request("http://d1/query?resultsFormat=NONE", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-cf-d1-session-commit-token": "bm-1",
      "x-vrt-epoch": "9",
      "x-vrt-seq": "9",
      "x-vrt-evil": "1",
    },
    body: '{"sql":"select 1"}',
  });
  const resp = await facade.fetch(req);
  assert.equal(resp.status, 200);
  const u = new URL(seen.url);
  // props bindingId anchors the path — never the env key name.
  assert.equal(u.pathname, "/internal/bindings/v1/d1/w-alpha-1~KV/query");
  assert.equal(u.search, "?resultsFormat=NONE");
  const h = new Headers(seen.headers);
  assert.equal(h.get("x-cf-d1-session-commit-token"), "bm-1");
  // Every inbound x-vrt-* is stripped.
  assert.equal(h.get("x-vrt-epoch"), null);
  assert.equal(h.get("x-vrt-seq"), null);
  assert.equal(h.get("x-vrt-evil"), null);
  // Identity headers come from props.
  assert.equal(h.get("x-open-compute-binding-id"), "w-alpha-1~KV");
  // T34d: provider's request-identity parse REQUIRES an RFC4122
  // `x-open-compute-request-id`; the facade mints one per request
  // (regression: its absence was the standalone e2e 500 root cause).
  assert.match(
    h.get("x-open-compute-request-id") ?? "",
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
  );
});

test("inbound x-open-compute-* is stripped; request-id is minted per request", async () => {
  let seen1, seen2;
  const handler = async (record) => {
    if (!seen1) seen1 = record; else seen2 = record;
    return new Response("[]", { headers: { "content-type": "application/json" } });
  };
  const { facade } = makeFacade({}, handler);
  const req = () => new Request("http://d1/query", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      // Forgery probes — must never reach the provider.
      "x-open-compute-request-id": "00000000-0000-0000-0000-000000000000",
      "x-open-compute-binding-id": "forged~EVIL",
      "x-open-compute-worker-id": "forged",
    },
    body: '{"sql":"select 1"}',
  });
  await facade.fetch(req());
  await facade.fetch(req());
  const h1 = new Headers(seen1.headers);
  // Forged identity headers are gone; host-minted ones won.
  assert.equal(h1.get("x-open-compute-binding-id"), "w-alpha-1~KV");
  const id1 = h1.get("x-open-compute-request-id");
  const id2 = new Headers(seen2.headers).get("x-open-compute-request-id");
  assert.match(id1 ?? "", /^[0-9a-f-]{36}$/);
  assert.match(id2 ?? "", /^[0-9a-f-]{36}$/);
  assert.notEqual(id1, id2);
  assert.notEqual(id1, "00000000-0000-0000-0000-000000000000");
});

test("facade failures return the D1 error surface, never uncaught", async () => {
  // Backend fetch rejecting → {success:false,error:"D1_ERROR: …"}.
  const { facade } = makeFacade({}, async () => {
    throw new Error("backend boom");
  });
  const r = await facade.fetch(new Request("http://d1/query", {
    method: "POST", body: '{"sql":"select 1"}',
  }));
  const body = await r.json();
  assert.equal(body.success, false);
  assert.match(body.error, /^D1_ERROR: /);
  // Provider non-ok (denial/5xx) → bounded D1 error surface, not the
  // raw status page d1-api would throw verbatim.
  const { facade: f2 } = makeFacade({}, async () => new Response("nope", { status: 403 }));
  const r2 = await f2.fetch(new Request("http://d1/query", {
    method: "POST", body: '{"sql":"select 1"}',
  }));
  const b2 = await r2.json();
  assert.equal(b2.success, false);
  assert.match(b2.error, /D1_ERROR: backend denied/);
  // Forged props → fail closed WITHOUT reaching the backend.
  let reached = false;
  const bad = new D1Facade({ props: { bindingId: "x" } }, {
    BINDING_BACKEND: { fetch: () => { reached = true; return new Response("[]"); } },
  });
  const r3 = await bad.fetch(new Request("http://d1/query", { method: "POST", body: "{}" }));
  const b3 = await r3.json();
  assert.equal(b3.success, false);
  assert.equal(reached, false);
});

test("binding name independence: facade uses props.bindingId only", async () => {
  let seen;
  const { facade } = makeFacade(
    { bindingId: "w-9~TOTALLY_OTHER_NAME" },
    async (record) => {
      seen = new URL(record.url).pathname;
      return new Response("[]", { headers: { "content-type": "application/json" } });
    },
  );
  // env has no D1-ish name at all — facade never reads it.
  await facade.fetch(new Request("http://d1/execute", {
    method: "POST", body: '{"sql":"select 1"}',
  }));
  assert.equal(seen, "/internal/bindings/v1/d1/w-9~TOTALLY_OTHER_NAME/execute");
});

test("rejects non-POST and unknown leaves", async () => {
  const { facade } = makeFacade();
  const r1 = await facade.fetch(new Request("http://d1/query"));
  assert.equal(r1.status, 404);
  const r2 = await facade.fetch(new Request("http://d1/nope", { method: "POST", body: "{}" }));
  assert.equal(r2.status, 404);
});

test("RV-R71: a hung backend is cut at the deadline with a retryable D1_ERROR", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { facade } = makeFacade({}, () => new Promise(() => {}));
  const pending = facade.fetch(
    new Request("http://d1/query", { method: "POST", body: '{"sql":"select 1"}' }),
  );
  await new Promise((r) => setImmediate(r));
  t.mock.timers.tick(25_000);
  const resp = await pending;
  const body = await resp.json();
  assert.equal(body.success, false);
  assert.match(body.error, /^D1_ERROR: backend timeout \(504\)/);
});

test("RV-R71: a rejecting backend yields a retryable D1_ERROR, not success", async () => {
  const { facade } = makeFacade({}, async () => {
    throw new Error("socket closed");
  });
  const resp = await facade.fetch(
    new Request("http://d1/query", { method: "POST", body: '{"sql":"select 1"}' }),
  );
  const body = await resp.json();
  assert.equal(body.success, false);
  assert.match(body.error, /backend error \(502\)/);
});
