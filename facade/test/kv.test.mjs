import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import {
  DIGEST,
  assertBindingError,
  assertIdentityHeaders,
  bytes,
  errorResponse,
  jsonResponse,
  kvb1,
  kvs1,
  kvJson,
  makeBackend,
  makeProps,
  facadeModule,
} from "./helpers.mjs";

const { KVNamespace } = await import(facadeModule("facade-kv.js"));

function kv(handler, props = makeProps()) {
  const { fetcher, requests } = makeBackend(handler);
  const ns = new KVNamespace({ props }, { BINDING_BACKEND: fetcher });
  return { ns, requests };
}

const KV_PATH = (props, op) =>
  `http://binding-backend/internal/bindings/v1/kv/${props.bindingId}/${op}`;

test("get(text) issues POST get with identity headers and decodes KVS1", async () => {
  const props = makeProps();
  const { ns, requests } = kv(() => kvs1(bytes("hello")), props);
  assert.equal(await ns.get("k1"), "hello");
  assert.equal(requests.length, 1);
  const req = requests[0];
  assert.equal(req.url, KV_PATH(props, "get"));
  assert.equal(req.method, "POST");
  assert.equal(
    req.headers["content-type"],
    "application/vnd.open-compute.kv.v1+frame",
  );
  assertIdentityHeaders(req.headers, props);
  assert.deepEqual(JSON.parse(new TextDecoder().decode(req.body)), {
    keys: ["k1"],
  });
});

test("get(json/arrayBuffer/stream) decode paths", async () => {
  const { ns } = kv(() => kvs1(bytes('{"a":1}')));
  assert.deepEqual(await ns.get("k", { type: "json" }), { a: 1 });
  const { ns: ns2 } = kv(() => kvs1(bytes("abc")));
  const buf = await ns2.get("k", { type: "arrayBuffer" });
  assert.deepEqual(new Uint8Array(buf), bytes("abc"));
  const { ns: ns3 } = kv(() => kvs1(bytes("xy")));
  const stream = await ns3.get("k", "stream");
  const text = await new Response(stream).text();
  assert.equal(text, "xy");
});

test("get miss returns null", async () => {
  const { ns } = kv(() => kvs1(null));
  assert.equal(await ns.get("missing"), null);
});

test("get many issues get-many and decodes KVB1 into Map", async () => {
  const props = makeProps();
  const { ns, requests } = kv(
    () => kvb1([{ value: bytes("v1") }, { value: null }, { value: bytes('{"x":2}'), meta: { m: 1 } }]),
    props,
  );
  const result = await ns.get(["a", "b", "c"]);
  assert.ok(result instanceof Map);
  assert.equal(result.get("a"), "v1");
  assert.equal(result.get("b"), null);
  assert.equal(result.get("c"), '{"x":2}');
  assert.equal(requests[0].url, KV_PATH(props, "get-many"));
  assert.deepEqual(JSON.parse(new TextDecoder().decode(requests[0].body)), {
    keys: ["a", "b", "c"],
  });
});

test("getWithMetadata returns value+metadata from KVS1", async () => {
  const { ns } = kv(() => kvs1(bytes("v"), { meta: { tag: "t" }, expiresMs: 1893456000000n }));
  const result = await ns.getWithMetadata("k");
  assert.deepEqual(result, {
    value: "v",
    metadata: { tag: "t" },
    cacheStatus: null,
  });
});

test("put sends framed header+value and honors options", async () => {
  const props = makeProps();
  const { ns, requests } = kv(() => new Response(null, { status: 200 }), props);
  await ns.put("k", "value-9", { expirationTtl: 120, metadata: { a: 1 } });
  assert.equal(requests.length, 1);
  const req = requests[0];
  assert.equal(req.url, KV_PATH(props, "put"));
  const view = new DataView(req.body.buffer, req.body.byteOffset, req.body.byteLength);
  const headerLen = view.getUint32(0);
  const header = JSON.parse(
    new TextDecoder().decode(req.body.subarray(4, 4 + headerLen)),
  );
  assert.deepEqual(header, {
    key: "k",
    expirationTtl: 120,
    metadata: { a: 1 },
    metadataPresent: true,
  });
  assert.deepEqual(req.body.subarray(4 + headerLen), bytes("value-9"));
});

test("delete sends delete op with key json", async () => {
  const props = makeProps();
  const { ns, requests } = kv(() => new Response(null, { status: 200 }), props);
  await ns.delete("k");
  assert.equal(requests[0].url, KV_PATH(props, "delete"));
  assert.deepEqual(JSON.parse(new TextDecoder().decode(requests[0].body)), {
    key: "k",
  });
});

test("list sends prefix/limit/cursor and maps response", async () => {
  const props = makeProps();
  const { ns, requests } = kv(
    () =>
      kvJson({
        keys: [
          { name: "a", expiration: 1893456000, metadata: { x: 1 } },
          { name: "b", expiration: null, metadata: null },
        ],
        list_complete: false,
        cursor: "cursor-1",
      }),
    props,
  );
  const result = await ns.list({ prefix: "a", limit: 10 });
  assert.equal(requests[0].url, KV_PATH(props, "list"));
  assert.deepEqual(JSON.parse(new TextDecoder().decode(requests[0].body)), {
    prefix: "a",
    limit: 10,
    cursor: null,
  });
  assert.deepEqual(result, {
    keys: [{ name: "a", expiration: 1893456000, metadata: { x: 1 } }, { name: "b" }],
    list_complete: false,
    cursor: "cursor-1",
    cacheStatus: null,
  });
});

test("backend error code maps to typed binding error", async () => {
  const { ns, requests } = kv(() => errorResponse("KV_KEY_INVALID", 400));
  await assertBindingError(ns.get("k"), "KV_KEY_INVALID");
  assert.equal(requests.length, 1);
});

test("backend failure without error header maps to BINDING_PROTOCOL_ERROR", async () => {
  const { ns } = kv(() => new Response("oops", { status: 500 }));
  await assertBindingError(ns.get("k"), "BINDING_PROTOCOL_ERROR");
});

test("permissions.read=false rejects before any request", async () => {
  const props = makeProps({ permissions: { read: false, write: true } });
  const { ns, requests } = kv(() => kvs1(bytes("v")), props);
  await assertBindingError(ns.get("k"), "BINDING_PERMISSION_DENIED");
  assert.equal(requests.length, 0);
});

test("permissions.write=false rejects put before any request", async () => {
  const props = makeProps({ permissions: { read: true, write: false } });
  const { ns, requests } = kv(() => new Response(null, { status: 200 }), props);
  await assertBindingError(ns.put("k", "v"), "BINDING_PERMISSION_DENIED");
  assert.equal(requests.length, 0);
});

test("missing props fail closed at construction, no request issued", async () => {
  for (const bad of [
    undefined,
    {},
    makeProps({ workerId: 1 }),
    makeProps({ namespaceResourceId: "" }),
    makeProps({ bindingId: null }),
    makeProps({ versionId: 42 }),
    makeProps({ descriptorSha256: "AB".repeat(32) }),
    makeProps({ descriptorSha256: "z".repeat(64) }),
    makeProps({ resourceSpecGeneration: 0 }),
    makeProps({ resourceSpecGeneration: 1.5 }),
    makeProps({ resourceSpecGeneration: "9" }),
    makeProps({ instanceId: "" }),
    { ...makeProps(), permissions: { read: "yes", write: true } },
  ]) {
    const { fetcher, requests } = makeBackend(() => kvs1(bytes("v")));
    assert.throws(
      () => new KVNamespace({ props: bad }, { BINDING_BACKEND: fetcher }),
      (error) => error.stableCode === "BINDING_PROTOCOL_ERROR",
      `props ${JSON.stringify(bad)} must be rejected`,
    );
    assert.equal(requests.length, 0);
  }
});

test("client-side validation errors never reach the backend", async () => {
  const { ns, requests } = kv(() => kvs1(bytes("v")));
  await assertBindingError(ns.get(""), "KV_KEY_INVALID");
  await assertBindingError(ns.get("."), "KV_KEY_INVALID");
  await assertBindingError(ns.get("k", { type: "bogus" }), "KV_INVALID_OPTIONS");
  await assertBindingError(ns.list({ limit: 0 }), "KV_INVALID_OPTIONS");
  assert.equal(requests.length, 0);
});

test("fetch() entrypoint serves the native KV wire (list)", async () => {
  const { ns } = kv(() =>
    kvJson({ keys: [{ name: "a", expiration: null, metadata: null }], list_complete: true, cursor: null }),
  );
  const response = await ns.fetch(
    new Request("http://fake-host/?key_count_limit=10"),
  );
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.keys.length, 1);
  assert.equal(body.keys[0].name, "a");
});

test("explicit cacheTtl caches reads and writes invalidate them", async () => {
  const props = makeProps();
  let gets = 0;
  const { ns } = kv((req) => {
    if (req.url.endsWith("/get")) {
      gets++;
      return kvs1(bytes("cached"));
    }
    return new Response(null, { status: 200 });
  }, props);
  assert.equal(await ns.get("k", { cacheTtl: 60 }), "cached");
  assert.equal(await ns.get("k", { cacheTtl: 60 }), "cached");
  assert.equal(gets, 1);
  await ns.put("k", "next");
  assert.equal(await ns.get("k", { cacheTtl: 60 }), "cached");
  assert.equal(gets, 2);
});
