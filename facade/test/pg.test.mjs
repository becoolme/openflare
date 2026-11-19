import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { assertBindingError, makeBackend, makeProps, jsonResponse, errorResponse,
  facadeModule,
} from "./helpers.mjs";

const { PgTransport } = await import(facadeModule("facade-pg.js"));

function pg(props = makeProps({ bindingId: "w-alpha-1~DB", namespaceResourceId: "vrtpg:tenant-7" }), handler) {
  const { fetcher, requests } = makeBackend(
    handler ?? (() => jsonResponse({ rows: [["1"]], rowCount: 1 })),
  );
  const transport = new PgTransport({ props }, { BINDING_BACKEND: fetcher });
  return { transport, requests };
}

const PG_PATH = "/internal/bindings/v1/pg/w-alpha-1~DB/sql";

test("query forwards Neon wire body to the provider /sql route", async () => {
  const { transport, requests } = pg();
  const out = await transport.query("select $1::int", [1]);
  assert.equal(requests.length, 1);
  const req = requests[0];
  assert.equal(req.method, "POST");
  assert.ok(req.url.endsWith(PG_PATH), req.url);
  assert.deepEqual(JSON.parse(new TextDecoder().decode(req.body)), {
    query: "select $1::int",
    params: [1],
  });
  // Identity headers come from props (minted host-side); a request-id
  // is required by the provider's identity parse.
  assert.equal(req.headers["x-open-compute-binding-id"], "w-alpha-1~DB");
  assert.ok(req.headers["x-open-compute-request-id"]);
  assert.deepEqual(out, { rows: [["1"]], rowCount: 1 });
});

test("execute is an alias of query", async () => {
  const { transport, requests } = pg();
  await transport.execute("select 1", []);
  assert.equal(requests.length, 1);
});

test("transaction emits a batch with neon-batch headers", async () => {
  const { transport, requests } = pg();
  await transport.transaction(
    [
      { sql: "insert into t values ($1)", params: [1] },
      { sql: "select count(*) from t", params: [] },
    ],
    { isolationLevel: "repeatable read", readOnly: true },
  );
  const req = requests[0];
  assert.deepEqual(JSON.parse(new TextDecoder().decode(req.body)).queries, [
    { query: "insert into t values ($1)", params: [1] },
    { query: "select count(*) from t", params: [] },
  ]);
  assert.equal(req.headers["neon-batch-isolation-level"], "repeatable read");
  assert.equal(req.headers["neon-batch-read-only"], "true");
});

test("fetch() acts as a neon() fetchFunction — body-only passthrough", async () => {
  const { transport, requests } = pg();
  const neonReq = new Request("https://pg.neon.local/sql", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      // Forgery attempts must never reach the provider.
      "x-open-compute-worker-id": "evil",
      "x-vrt-forge": "1",
    },
    body: JSON.stringify({ query: "select 1", params: [] }),
  });
  const resp = await transport.fetch(neonReq);
  assert.equal(resp.status, 200);
  const req = requests[0];
  assert.equal(req.headers["x-open-compute-worker-id"], "w-alpha-1");
  assert.equal(req.headers["x-vrt-forge"], undefined);
});

test("fetch() batch form forwards {queries}", async () => {
  const { transport, requests } = pg();
  const resp = await transport.fetch(
    new Request("https://x/sql", {
      method: "POST",
      body: JSON.stringify({ queries: [{ query: "select 1", params: [] }] }),
    }),
  );
  assert.equal(resp.status, 200);
  assert.deepEqual(JSON.parse(new TextDecoder().decode(requests[0].body)), {
    queries: [{ query: "select 1", params: [] }],
  });
});

test("backend errors surface as typed binding errors", async () => {
  const { transport } = pg(
    makeProps({ bindingId: "w-alpha-1~DB", namespaceResourceId: "vrtpg:t" }),
    () => errorResponse("PG_WRITE_FENCED", 409),
  );
  await assertBindingError(transport.query("delete from t"), "PG_WRITE_FENCED");
});

test("connect()/prepare() remain capability-unsupported (TCP is T-future)", async () => {
  const { transport, requests } = pg();
  await assertBindingError(transport.connect(), "BINDING_CAPABILITY_UNSUPPORTED");
  await assertBindingError(transport.prepare(), "BINDING_CAPABILITY_UNSUPPORTED");
  assert.equal(requests.length, 0);
});

test("fetch() non-POST / bad JSON / oversized fail typed, no backend hit", async () => {
  const { transport, requests } = pg();
  const get = await transport.fetch(new Request("https://x/sql", { method: "GET" }));
  assert.equal(get.status, 422);
  const bad = await transport.fetch(
    new Request("https://x/sql", { method: "POST", body: "not json" }),
  );
  assert.equal(bad.status, 422);
  const big = await transport.fetch(
    new Request("https://x/sql", {
      method: "POST",
      body: JSON.stringify({ query: "x".repeat(300 * 1024) }),
    }),
  );
  assert.equal(big.status, 422);
  assert.equal(requests.length, 0);
});

test("query param bounds are enforced (protocol errors)", async () => {
  const { transport, requests } = pg();
  await assertBindingError(
    transport.query("select $1", Array.from({ length: 300 }, (_, i) => i)),
    "BINDING_LIMIT_EXCEEDED",
  );
  await assertBindingError(transport.query(42), "BINDING_PROTOCOL_ERROR");
  assert.equal(requests.length, 0);
});

test("missing or forged props fail closed at construction", async () => {
  for (const bad of [
    undefined,
    {},
    makeProps({ bindingId: "x", namespaceResourceId: undefined }),
    makeProps({ resourceSpecGeneration: 0 }),
  ]) {
    const { fetcher, requests } = makeBackend(() => new Response("never"));
    assert.throws(
      () => new PgTransport({ props: bad }, { BINDING_BACKEND: fetcher }),
      (error) => error.stableCode === "BINDING_PROTOCOL_ERROR",
    );
    assert.equal(requests.length, 0);
  }
});

test("RV-R71: hung provider is cut at the deadline with BINDING_BACKEND_TIMEOUT", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { transport } = pg(undefined, () => new Promise(() => {}));
  const pending = transport.query("select 1", []).then(
    () => "resolved",
    (e) => e.stableCode,
  );
  await new Promise((r) => setImmediate(r));
  t.mock.timers.tick(25_000);
  assert.equal(await pending, "BINDING_BACKEND_TIMEOUT");
});

test("RV-R71: transaction transport failure is BINDING_BACKEND_UNAVAILABLE", async () => {
  const { transport } = pg(undefined, () => {
    throw new Error("boom");
  });
  await assert.rejects(
    () => transport.transaction([{ query: "select 1", params: [] }]),
    (e) => e.stableCode === "BINDING_BACKEND_UNAVAILABLE",
  );
});
