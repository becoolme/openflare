import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import {
  assertBindingError,
  errorResponse,
  makeBackend,
  makeProps,
  facadeModule,
} from "./helpers.mjs";

const { AssetTransport } = await import(facadeModule("facade-assets.js"));

const ASSET_URL = "http://binding-backend/internal/assets/v1/fetch";

function assets(handler, props = makeProps({ namespaceResourceId: "manifest-digest-1" })) {
  const { fetcher, requests } = makeBackend(handler);
  const transport = new AssetTransport({ props }, { BINDING_BACKEND: fetcher });
  return { transport, requests, props };
}

function wire(headers = [["accept", "text/html"]]) {
  return { url: "https://w--t.vibeploy.app/app.js", method: "GET", headers };
}

test("fetchAsset GET posts to assets fetch endpoint with identity+asset headers", async () => {
  const { transport, requests, props } = assets(
    () => new Response("body-1", { status: 200 }),
  );
  const response = await transport.fetchAsset(wire());
  assert.equal(response.status, 200);
  assert.equal(await response.text(), "body-1");
  const req = requests[0];
  assert.equal(req.url, ASSET_URL);
  assert.equal(req.method, "POST");
  assert.equal(req.headers["x-open-compute-asset-method"], "GET");
  assert.equal(req.headers["x-open-compute-asset-url"], "https://w--t.vibeploy.app/app.js");
  assert.equal(req.headers["x-open-compute-binding-id"], props.bindingId);
  assert.equal(req.headers["x-open-compute-worker-id"], props.workerId);
  assert.equal(req.headers["x-open-compute-version-id"], props.versionId);
  assert.equal(req.headers["x-open-compute-descriptor-sha256"], props.descriptorSha256);
  assert.equal(req.headers["x-open-compute-namespace-resource-id"], "manifest-digest-1");
  assert.equal(
    req.headers["x-open-compute-resource-spec-generation"],
    String(props.resourceSpecGeneration),
  );
  assert.match(req.headers["x-open-compute-request-id"], /^[0-9a-f-]{36}$/);
  assert.equal(req.headers["accept"], "text/html");
});

test("tenant-supplied x-open-compute-* headers are stripped, not forwarded", async () => {
  const { transport, requests } = assets(() => new Response("ok"));
  await transport.fetchAsset(
    wire([
      ["x-open-compute-worker-id", "forged-worker"],
      ["x-open-compute-asset-method", "DELETE"],
      ["x-forwarded-for", "1.2.3.4"],
      ["x-open-compute-instance-id", "evil"],
      ["x-open-compute-custom-future", "sneak"],
    ]),
  );
  const req = requests[0];
  assert.notEqual(req.headers["x-open-compute-worker-id"], "forged-worker");
  assert.equal(req.headers["x-open-compute-asset-method"], "GET");
  assert.equal(req.headers["x-forwarded-for"], undefined);
  assert.equal(req.headers["x-open-compute-custom-future"], undefined);
  assert.equal(req.headers["x-open-compute-instance-id"], undefined);
  assert.equal(req.headers["x-open-compute-resource-spec-generation"], "9");
});

test("fetch(request) uses the same path and strips internal headers", async () => {
  const { transport, requests, props } = assets(() => new Response("ok"));
  await transport.fetch(
    new Request("https://w--t.vibeploy.app/", {
      method: "GET",
      headers: { "x-open-compute-descriptor-sha256": "0".repeat(64) },
    }),
  );
  assert.equal(requests[0].url, ASSET_URL);
  assert.equal(
    requests[0].headers["x-open-compute-descriptor-sha256"],
    props.descriptorSha256,
  );
});

test("non-GET/HEAD is an explicit unsupported error, no request", async () => {
  const { transport, requests } = assets(() => new Response("ok"));
  await assertBindingError(
    transport.fetchAsset({ ...wire(), method: "POST" }),
    "BINDING_CAPABILITY_UNSUPPORTED",
  );
  await assertBindingError(
    transport.fetch(new Request("https://w--t.vibeploy.app/", { method: "PUT" })),
    "BINDING_CAPABILITY_UNSUPPORTED",
  );
  assert.equal(requests.length, 0);
});

test("malformed wire input fails closed", async () => {
  const { transport, requests } = assets(() => new Response("ok"));
  for (const bad of [
    null,
    {},
    { url: 1, method: "GET", headers: [] },
    { url: "https://x/", method: "GET", headers: "not-array" },
    { url: "https://x/", method: "GET", headers: [["a"]] },
    { url: "https://x/", method: "GET", headers: [["a", "b"], [1, "b"]] },
    { url: "https://x/", method: "GET", headers: Array.from({ length: 300 }, () => ["a", "b"]) },
  ]) {
    await assertBindingError(transport.fetchAsset(bad), "BINDING_PROTOCOL_ERROR");
  }
  assert.equal(requests.length, 0);
});

test("backend error code propagates typed", async () => {
  const { transport } = assets(() => errorResponse("ASSET_STORAGE_UNAVAILABLE", 503));
  await assertBindingError(transport.fetchAsset(wire()), "ASSET_STORAGE_UNAVAILABLE");
});

test("representation-length header is preserved on passthrough", async () => {
  const { transport } = assets(
    () =>
      new Response("hello", {
        status: 200,
        headers: { "x-open-compute-asset-representation-length": "5", "content-length": "5" },
      }),
  );
  const response = await transport.fetchAsset(wire());
  assert.equal(response.headers.get("x-open-compute-asset-representation-length"), "5");
});

test("missing or forged props fail closed at construction", async () => {
  for (const bad of [
    undefined,
    {},
    makeProps({ namespaceResourceId: "" }),
    makeProps({ descriptorSha256: "xx" }),
  ]) {
    const { fetcher, requests } = makeBackend(() => new Response("ok"));
    assert.throws(
      () => new AssetTransport({ props: bad }, { BINDING_BACKEND: fetcher }),
      (error) => error.stableCode === "BINDING_PROTOCOL_ERROR",
    );
    assert.equal(requests.length, 0);
  }
});

// T13/L2: response header stripping + error-code whitelist.

test("backend x-open-compute-* response headers are stripped", async () => {
  const { transport } = assets(
    () =>
      new Response("ok", {
        status: 200,
        headers: {
          "content-type": "text/plain",
          "x-open-compute-internal-debug": "pid=1234",
          "x-open-compute-error-code": "ASSET_NOT_FOUND",
          "x-internal-trace": "abc",
        },
      }),
  );
  const response = await transport.fetchAsset(wire());
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-type"), "text/plain");
  for (const name of response.headers.keys()) {
    assert.ok(!name.startsWith("x-open-compute-") || name === "x-open-compute-asset-representation-length", `leaked ${name}`);
  }
  assert.equal(response.headers.get("x-internal-trace"), null);
});

test("_headers-touched response headers are forwarded, internal names are not", async () => {
  const { transport } = assets(
    () =>
      new Response("ok", {
        status: 200,
        headers: {
          "content-type": "text/plain",
          "x-open-compute-asset-header-names":
            "content-security-policy,x-frame-options,x-open-compute-foo,connection",
          "content-security-policy": "default-src 'self'",
          "x-frame-options": "DENY",
          "x-open-compute-foo": "forged",
          "connection": "close",
        },
      }),
  );
  const response = await transport.fetchAsset(wire());
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-security-policy"), "default-src 'self'");
  assert.equal(response.headers.get("x-frame-options"), "DENY");
  assert.equal(response.headers.get("x-open-compute-foo"), null);
  assert.equal(response.headers.get("connection"), null);
  for (const name of response.headers.keys()) {
    assert.ok(!name.startsWith("x-open-compute-"), `leaked ${name}`);
  }
});

test("_headers-touched names are forwarded on redirect and 304 responses", async () => {
  // The provider applies `_headers` to html-handling / `_redirects` 3xx and
  // 304s too (Cloudflare `attachCustomHeaders` runs for every response), so
  // the facade must forward the touched names regardless of status.
  for (const status of [301, 307, 304]) {
    const headers = {
      "x-open-compute-asset-header-names": "x-custom-foo-header",
      "x-custom-foo-header": "Custom-Foo-Value",
    };
    if (status === 304) headers.etag = '"abc"';
    else headers.location = "/bar";
    const { transport } = assets(() => new Response(null, { status, headers }));
    const response = await transport.fetchAsset(wire());
    assert.equal(response.status, status);
    assert.equal(
      response.headers.get("x-custom-foo-header"),
      "Custom-Foo-Value",
      `status ${status}`,
    );
    if (status !== 304) {
      assert.equal(response.headers.get("location"), "/bar", `status ${status}`);
    }
    for (const name of response.headers.keys()) {
      assert.ok(!name.startsWith("x-open-compute-"), `leaked ${name}`);
    }
  }
});

test("unknown backend error codes collapse to a whitelist entry", async () => {
  const { transport } = assets(() => errorResponse("SECRET_INTERNAL_FUZZ", 500));
  await assertBindingError(transport.fetchAsset(wire()), "ASSET_STORAGE_UNAVAILABLE");
});

// Cloudflare compat: a missing asset is a 404 Response, never a throw.
test("typed ASSET_NOT_FOUND 404 maps to a plain 404 Response (GET)", async () => {
  const { transport } = assets(() => errorResponse("ASSET_NOT_FOUND", 404));
  const response = await transport.fetchAsset(wire());
  assert.equal(response.status, 404);
  assert.equal(await response.text(), "Not Found");
  for (const name of response.headers.keys()) {
    assert.ok(!name.startsWith("x-open-compute-"), `leaked ${name}`);
  }
});

test("typed ASSET_NOT_FOUND 404 maps to a bodyless 404 (HEAD)", async () => {
  const { transport } = assets(() => errorResponse("ASSET_NOT_FOUND", 404));
  const response = await transport.fetchAsset({ ...wire(), method: "HEAD" });
  assert.equal(response.status, 404);
  assert.equal(await response.text(), "");
  for (const name of response.headers.keys()) {
    assert.ok(!name.startsWith("x-open-compute-"), `leaked ${name}`);
  }
});

test("404-page body served with status 404 passes through", async () => {
  const { transport } = assets(
    () => new Response("<h1>gone</h1>", { status: 404, headers: { "content-type": "text/html" } }),
  );
  const response = await transport.fetchAsset(wire());
  assert.equal(response.status, 404);
  assert.equal(await response.text(), "<h1>gone</h1>");
});

test("other typed error codes still throw", async () => {
  for (const [code, status] of [
    ["ASSET_STORAGE_UNAVAILABLE", 503],
    ["ASSET_TOO_LARGE", 413],
    ["ASSET_INTERNAL_ERROR", 500],
  ]) {
    const { transport } = assets(() => errorResponse(code, status));
    await assertBindingError(transport.fetchAsset(wire()), code);
  }
  // ASSET_NOT_FOUND on a non-404 status is not the typed miss: still throws.
  const { transport } = assets(() => errorResponse("ASSET_NOT_FOUND", 500));
  await assertBindingError(transport.fetchAsset(wire()), "ASSET_NOT_FOUND");
});
