import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { makeProps, makeBackend, facadeModule } from "./helpers.mjs";

const { VectorizeFacade } = await import(facadeModule("facade-vectorize.js"));

function makeFacade(propsOverrides = {}, handler = null) {
  const backend = makeBackend(
    handler ?? (async () => new Response('{"count":0,"matches":[]}', {
      headers: { "content-type": "application/json" },
    })),
  );
  const env = { BINDING_BACKEND: backend.fetcher };
  const facade = new VectorizeFacade({ props: makeProps(propsOverrides) }, env);
  return { facade, backend };
}

test("POST /query forwards to provider with signed identity headers", async () => {
  let seen;
  const { facade } = makeFacade({}, async (record) => {
    seen = record;
    return new Response('{"count":0,"matches":[]}', {
      headers: { "content-type": "application/json" },
    });
  });
  const req = new Request("http://vec/query", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-vrt-evil": "1",
      "x-open-compute-spoof": "x",
    },
    body: '{"vector":[1,0,0],"topK":5}',
  });
  const resp = await facade.fetch(req);
  assert.equal(resp.status, 200);
  const u = new URL(seen.url);
  assert.equal(
    u.pathname,
    "/internal/bindings/v1/vectorize/w-alpha-1~KV/query",
  );
  assert.equal(seen.method, "POST");
  // Forged trusted-context headers stripped; request id minted.
  assert.equal(seen.headers["x-vrt-evil"], undefined);
  assert.equal(seen.headers["x-open-compute-spoof"], undefined);
  assert.match(seen.headers["x-open-compute-request-id"], /^[0-9a-f-]{36}$/);
  assert.equal(new TextDecoder().decode(seen.body), '{"vector":[1,0,0],"topK":5}');
});

test("GET /info forwards with no body", async () => {
  let seen;
  const { facade } = makeFacade({}, async (record) => {
    seen = record;
    return new Response('{"vectorCount":7,"dimensions":3,"processedUpToDatetime":0,"processedUpToMutation":"5"}');
  });
  const resp = await facade.fetch(new Request("http://vec/info"));
  assert.equal(resp.status, 200);
  const u = new URL(seen.url);
  assert.equal(u.pathname, "/internal/bindings/v1/vectorize/w-alpha-1~KV/info");
  assert.equal(seen.method, "GET");
});

test("all v2 ops route; unknown leaf returns the {code,error} envelope", async () => {
  const { facade, backend } = makeFacade();
  for (const op of ["query", "insert", "upsert", "getByIds", "deleteByIds"]) {
    const resp = await facade.fetch(
      new Request(`http://vec/${op}`, { method: "POST", body: "{}" }),
    );
    assert.equal(resp.status, 200, op);
  }
  const bad = await facade.fetch(
    new Request("http://vec/nope", { method: "POST", body: "{}" }),
  );
  assert.equal(bad.status, 404);
  const err = await bad.json();
  assert.equal(typeof err.code, "number");
  assert.equal(typeof err.error, "string");
  assert.equal(backend.requests.length, 5);
});

test("wrong method on a known op is rejected", async () => {
  const { facade } = makeFacade();
  const resp = await facade.fetch(new Request("http://vec/query"));
  assert.equal(resp.status, 404);
  const resp2 = await facade.fetch(
    new Request("http://vec/info", { method: "POST", body: "{}" }),
  );
  assert.equal(resp2.status, 404);
});

test("backend non-200 passes through unchanged", async () => {
  const { facade } = makeFacade({}, async () =>
    new Response('{"code":3010,"error":"vector dimensions mismatch"}', {
      status: 400,
      headers: { "content-type": "application/json" },
    }));
  const resp = await facade.fetch(
    new Request("http://vec/query", { method: "POST", body: '{"vector":[1]}' }),
  );
  assert.equal(resp.status, 400);
  assert.deepEqual(await resp.json(), {
    code: 3010,
    error: "vector dimensions mismatch",
  });
});
