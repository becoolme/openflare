// Shared test helpers: props factory, recording BINDING_BACKEND fake, and
// KVS1/KVB1/R2 frame encoders matching crates/service/src/binding_backend.
import assert from "node:assert/strict";

// Repo-relative module name → URL inside the published dist tree.
// Tests must import dist bytes — never the .ts sources — so `node --test`
// exercises exactly what the manifest addresses for workerd embed.
export const DIST = new URL("../dist/", import.meta.url);
export function distModule(name) {
  return new URL(name, DIST).href;
}
export const FACADE_DIR_URL = DIST;
export function facadeModule(base) {
  return distModule(`facade/${base}`);
}

export const DIGEST = "a".repeat(64);

export function makeProps(overrides = {}) {
  return {
    bindingId: "w-alpha-1~KV",
    versionId: "w-alpha-1:v3",
    workerId: "w-alpha-1",
    instanceId: "tenant-7",
    descriptorSha256: DIGEST,
    namespaceResourceId: "ns-42",
    resourceSpecGeneration: 9,
    permissions: { read: true, write: true },
    ...overrides,
  };
}

/** Fake Fetcher: records drained requests and delegates to `handler`. */
export function makeBackend(handler) {
  const requests = [];
  const fetcher = {
    async fetch(input, init = {}) {
      const body =
        init.body == null
          ? null
          : new Uint8Array(await new Response(init.body).arrayBuffer());
      const headers = {};
      new Headers(init.headers).forEach((value, key) => {
        headers[key] = value;
      });
      const record = { url: String(input), method: init.method ?? "GET", headers, body };
      requests.push(record);
      return handler(record, requests.length - 1);
    },
  };
  return { fetcher, requests };
}

export function jsonResponse(value, { status = 200, headers = {} } = {}) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export function errorResponse(code, status = 400) {
  return jsonResponse(
    { ok: false, error: { code, retryable: false, resultUnknown: false } },
    { status, headers: { "x-open-compute-error-code": code } },
  );
}

const enc = new TextEncoder();

function u32be(value) {
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, value);
  return out;
}

function i64be(value) {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigInt64(0, BigInt(value));
  return out;
}

function concat(parts) {
  const total = parts.reduce((sum, p) => sum + p.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

function entryRecord(value, metaBytes, expiresMs = -1n) {
  if (value === null) {
    return concat([
      new Uint8Array([0]),
      i64be(-1n),
      u32be(0xffffffff),
      u32be(0xffffffff),
    ]);
  }
  return concat([
    new Uint8Array([1]),
    i64be(expiresMs),
    metaBytes === null ? u32be(0xffffffff) : u32be(metaBytes.byteLength),
    ...(metaBytes === null ? [] : [metaBytes]),
    u32be(value.byteLength),
    value,
  ]);
}

/** KVS1 frame body for get / get-with-metadata. */
export function kvs1(value, { meta = null, expiresMs = -1n } = {}) {
  const metaBytes =
    meta === null ? null : enc.encode(JSON.stringify(meta));
  return new Response(
    concat([enc.encode("KVS1"), entryRecord(value, metaBytes, expiresMs)]),
    {
      headers: { "content-type": "application/vnd.open-compute.kv.v1+frame" },
    },
  );
}

/** KVB1 frame body for get-many; entries in request order. */
export function kvb1(entries) {
  const records = entries.map(({ value, meta = null, expiresMs = -1n }) =>
    entryRecord(value, meta === null ? null : enc.encode(JSON.stringify(meta)), expiresMs),
  );
  const count = new Uint8Array(2);
  new DataView(count.buffer).setUint16(0, entries.length);
  return new Response(
    concat([enc.encode("KVB1"), count, ...records]),
    { headers: { "content-type": "application/vnd.open-compute.kv.v1+frame" } },
  );
}

export function kvJson(value, init) {
  return jsonResponse(value, init);
}

export function r2Meta(overrides = {}) {
  return {
    key: "obj-1",
    version: "v-9",
    size: 5,
    etag: "etag-1",
    httpEtag: "http-etag-1",
    uploaded: 1700000000000,
    checksums: {},
    storageClass: "Standard",
    ...overrides,
  };
}

/** R2 get/uploadPart-style frame: <u32 len><json header><body bytes>. */
export function r2Frame(meta, body = undefined) {
  const header = enc.encode(JSON.stringify({ meta, hasBody: body !== undefined }));
  const payload = concat([u32be(header.byteLength), header, ...(body === undefined ? [] : [body])]);
  return new Response(payload, {
    headers: { "content-type": "application/vnd.open-compute.r2.v1+frame" },
  });
}

export function bytes(value) {
  return enc.encode(value);
}

export function streamOf(...chunks) {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

/** Assert a promise rejects with a typed binding error carrying `code`. */
export async function assertBindingError(promise, code) {
  const error = await promise.then(
    () => {
      throw new assert.AssertionError({
        message: `expected rejection with ${code}, promise resolved`,
      });
    },
    (rejected) => rejected,
  );
  assert.ok(error instanceof Error, "expected Error");
  assert.equal(
    error.stableCode ?? error.message,
    code,
    `expected stableCode ${code}, got ${error.message}`,
  );
  return error;
}

export const IDENTITY_HEADERS = [
  "x-open-compute-binding-id",
  "x-open-compute-version-id",
  "x-open-compute-worker-id",
  "x-open-compute-descriptor-sha256",
  "x-open-compute-namespace-resource-id",
  "x-open-compute-resource-spec-generation",
];

export function assertIdentityHeaders(headers, props) {
  assert.equal(headers["x-open-compute-binding-id"], props.bindingId);
  assert.equal(headers["x-open-compute-version-id"], props.versionId);
  assert.equal(headers["x-open-compute-worker-id"], props.workerId);
  assert.equal(headers["x-open-compute-descriptor-sha256"], props.descriptorSha256);
  assert.equal(headers["x-open-compute-namespace-resource-id"], props.namespaceResourceId);
  assert.equal(
    headers["x-open-compute-resource-spec-generation"],
    String(props.resourceSpecGeneration),
  );
  assert.match(headers["x-open-compute-request-id"] ?? "", /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
}
