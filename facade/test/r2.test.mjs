import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import {
  assertBindingError,
  assertIdentityHeaders,
  bytes,
  errorResponse,
  jsonResponse,
  makeBackend,
  makeProps,
  r2Frame,
  r2Meta,
  streamOf,
  facadeModule,
} from "./helpers.mjs";

const { R2Transport } = await import(facadeModule("facade-r2.js"));

function r2(handler, props = makeProps()) {
  const { fetcher, requests } = makeBackend(handler);
  const bucket = new R2Transport({ props }, { BINDING_BACKEND: fetcher });
  return { bucket, requests };
}

const R2_PATH = (props, op) =>
  `http://binding-backend/internal/bindings/v1/r2/${props.bindingId}/${op}`;
const JSON_CT = "application/vnd.open-compute.r2.v1+json";
const FRAME_CT = "application/vnd.open-compute.r2.v1+frame";

function frameParts(body) {
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  const headerLen = view.getUint32(0);
  const header = JSON.parse(
    new TextDecoder().decode(body.subarray(4, 4 + headerLen)),
  );
  return { header, payload: body.subarray(4 + headerLen) };
}

test("head issues POST head with JSON body and identity headers", async () => {
  const props = makeProps();
  const meta = r2Meta();
  const { bucket, requests } = r2(() => jsonResponse(meta), props);
  const result = await bucket.head("obj-1");
  assert.deepEqual(result, meta);
  const req = requests[0];
  assert.equal(req.url, R2_PATH(props, "head"));
  assert.equal(req.method, "POST");
  assert.equal(req.headers["content-type"], JSON_CT);
  assertIdentityHeaders(req.headers, props);
  assert.deepEqual(JSON.parse(new TextDecoder().decode(req.body)), {
    key: "obj-1",
  });
});

test("head miss (204) returns null", async () => {
  const { bucket } = r2(() => new Response(null, { status: 204 }));
  assert.equal(await bucket.head("none"), null);
});

test("get sends frame content-type and decodes framed meta+body", async () => {
  const props = makeProps();
  const meta = r2Meta({ size: 3 });
  const { bucket, requests } = r2(() => r2Frame(meta, bytes("abc")), props);
  const result = await bucket.get("obj-1", {});
  assert.equal(requests[0].url, R2_PATH(props, "get"));
  assert.equal(requests[0].headers["content-type"], FRAME_CT);
  assert.deepEqual(JSON.parse(new TextDecoder().decode(requests[0].body)), {
    key: "obj-1",
    options: {},
  });
  assert.deepEqual(result.meta, meta);
  const text = await new Response(result.body).text();
  assert.equal(text, "abc");
});

test("get miss (204) returns null; unmet condition returns meta without body", async () => {
  const { bucket } = r2(() => new Response(null, { status: 204 }));
  assert.equal(await bucket.get("none", {}), null);
  const meta = r2Meta();
  const { bucket: b2 } = r2(() => r2Frame(meta));
  const result = await b2.get("k", { onlyIf: { etagMatches: [], etagDoesNotMatch: [] } });
  assert.deepEqual(result, { meta });
});

test("put sends framed key+options+body and decodes metadata", async () => {
  const props = makeProps();
  const meta = r2Meta();
  const { bucket, requests } = r2(() => jsonResponse(meta), props);
  const options = {
    httpMetadata: { contentType: "text/plain" },
    customMetadata: { team: "a" },
  };
  const result = await bucket.put(
    "obj-1",
    streamOf(bytes("hello " ), bytes("world")),
    options,
  );
  assert.deepEqual(result, meta);
  const req = requests[0];
  assert.equal(req.url, R2_PATH(props, "put"));
  assert.equal(req.headers["content-type"], FRAME_CT);
  const { header, payload } = frameParts(req.body);
  assert.deepEqual(header, { key: "obj-1", options });
  assert.deepEqual(payload, bytes("hello world"));
});

test("put precondition miss (204) returns null", async () => {
  const { bucket } = r2(() => new Response(null, { status: 204 }));
  assert.equal(
    await bucket.put("k", streamOf(bytes("v")), {
      httpMetadata: {},
      customMetadata: {},
    }),
    null,
  );
});

test("delete sends key list (dedup/limits are provider-side)", async () => {
  const props = makeProps();
  const { bucket, requests } = r2(() => new Response(null, { status: 204 }), props);
  await bucket.delete(["a", "b"]);
  assert.equal(requests[0].url, R2_PATH(props, "delete"));
  assert.deepEqual(JSON.parse(new TextDecoder().decode(requests[0].body)), {
    keys: ["a", "b"],
  });
});

test("list sends options and validates response shape", async () => {
  const props = makeProps();
  const listed = {
    objects: [r2Meta({ key: "p/a" }), r2Meta({ key: "p/b" })],
    truncated: true,
    cursor: "cur-1",
    delimitedPrefixes: ["p/sub/"],
  };
  const { bucket, requests } = r2(() => jsonResponse(listed), props);
  const options = { prefix: "p/", limit: 5, include: ["httpMetadata"] };
  const result = await bucket.list(options);
  assert.equal(requests[0].url, R2_PATH(props, "list"));
  assert.deepEqual(JSON.parse(new TextDecoder().decode(requests[0].body)), options);
  assert.deepEqual(result, listed);
});

test("backend error code maps to typed binding error", async () => {
  const { bucket } = r2(() => errorResponse("R2_INVALID_OPTIONS", 400));
  await assertBindingError(bucket.head("k"), "R2_INVALID_OPTIONS");
});

test("unsupported capability error surfaces (multipart stub)", async () => {
  const { bucket } = r2(() => errorResponse("BINDING_CAPABILITY_UNSUPPORTED", 422));
  await assertBindingError(
    bucket.createMultipartUpload("k", { httpMetadata: {}, customMetadata: {} }),
    "BINDING_CAPABILITY_UNSUPPORTED",
  );
});

test("permission denied rejects before any request", async () => {
  const props = makeProps({ permissions: { read: true, write: false } });
  const { bucket, requests } = r2(() => new Response(null, { status: 204 }), props);
  await assertBindingError(
    bucket.put("k", streamOf(bytes("v")), { httpMetadata: {}, customMetadata: {} }),
    "BINDING_PERMISSION_DENIED",
  );
  assert.equal(requests.length, 0);
});

test("missing or forged props fail closed at construction", async () => {
  for (const bad of [
    undefined,
    {},
    makeProps({ bindingId: "" }),
    makeProps({ workerId: null }),
    makeProps({ namespaceResourceId: undefined }),
    makeProps({ descriptorSha256: "ff" }),
    makeProps({ resourceSpecGeneration: -1 }),
    makeProps({ permissions: null }),
  ]) {
    const { fetcher, requests } = makeBackend(() => jsonResponse(r2Meta()));
    assert.throws(
      () => new R2Transport({ props: bad }, { BINDING_BACKEND: fetcher }),
      (error) => error.stableCode === "BINDING_PROTOCOL_ERROR",
      `props ${JSON.stringify(bad)} must be rejected`,
    );
    assert.equal(requests.length, 0);
  }
});
