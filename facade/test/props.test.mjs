import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { DIGEST, makeBackend, makeProps,
  facadeModule,
} from "./helpers.mjs";

const { resourceProps, wrapBindingBackend, identityHeaders } = await import(
  facadeModule("props.js"),
);

test("resourceProps accepts the full vrt props contract", () => {
  const props = resourceProps(makeProps());
  assert.equal(props.bindingId, "w-alpha-1~KV");
  assert.equal(props.namespaceResourceId, "ns-42");
  assert.equal(props.resourceSpecGeneration, 9);
});

test("resourceProps rejects missing and mistyped fields", () => {
  const cases = [
    undefined,
    null,
    "x",
    [],
    {},
    makeProps({ bindingId: "" }),
    makeProps({ bindingId: 7 }),
    makeProps({ versionId: "" }),
    makeProps({ workerId: undefined }),
    makeProps({ instanceId: null }),
    makeProps({ descriptorSha256: DIGEST.toUpperCase() }),
    makeProps({ descriptorSha256: "g".repeat(64) }),
    makeProps({ descriptorSha256: "a".repeat(63) }),
    makeProps({ namespaceResourceId: "" }),
    makeProps({ resourceSpecGeneration: 0 }),
    makeProps({ resourceSpecGeneration: -3 }),
    makeProps({ resourceSpecGeneration: 1.5 }),
    makeProps({ resourceSpecGeneration: "9" }),
    makeProps({ resourceSpecGeneration: Number.MAX_SAFE_INTEGER + 1 }),
    makeProps({ bindingId: "a/b" }),
    makeProps({ bindingId: "a?x" }),
    makeProps({ bindingId: "a#x" }),
    makeProps({ bindingId: "a%2e" }),
    makeProps({ bindingId: "a b" }),
    makeProps({ bindingId: "a\tb" }),
    makeProps({ bindingId: "a\u007fb" }),
    makeProps({ versionId: "x\u0007y" }),
    makeProps({ workerId: "w\u001fy" }),
    makeProps({ instanceId: "i\u0000y" }),
    makeProps({ namespaceResourceId: "n\u000by" }),
    makeProps({ permissions: null }),
    makeProps({ permissions: "rw" }),
    { ...makeProps(), permissions: { read: true } },
    { ...makeProps(), permissions: { read: true, write: 1 } },
  ];
  for (const bad of cases) {
    assert.throws(
      () => resourceProps(bad),
      (error) => error.stableCode === "BINDING_PROTOCOL_ERROR",
      `props ${JSON.stringify(bad)} must be rejected`,
    );
  }
});

test("identityHeaders carries only validated props fields", () => {
  const props = resourceProps(makeProps());
  const headers = identityHeaders(props);
  assert.deepEqual(headers, {
    "x-open-compute-binding-id": props.bindingId,
    "x-open-compute-version-id": props.versionId,
    "x-open-compute-worker-id": props.workerId,
    "x-open-compute-descriptor-sha256": props.descriptorSha256,
    "x-open-compute-namespace-resource-id": props.namespaceResourceId,
    "x-open-compute-resource-spec-generation": "9",
  });
});

test("wrapBindingBackend identity headers override caller-supplied forgeries", async () => {
  const props = makeProps({ workerId: "w-real", namespaceResourceId: "ns-real" });
  const { fetcher, requests } = makeBackend(() => new Response("ok"));
  const wrapped = wrapBindingBackend(fetcher, props);
  await wrapped.fetch("http://binding-backend/x", {
    method: "POST",
    headers: {
      "x-open-compute-worker-id": "forged",
      "x-open-compute-namespace-resource-id": "other-ns",
      "x-open-compute-binding-id": "forged-binding",
      "x-open-compute-resource-spec-generation": "999999",
      "x-open-compute-version-id": "forged-version",
      "x-open-compute-descriptor-sha256": "0".repeat(64),
      "content-type": "application/json",
    },
    body: "{}",
  });
  const headers = requests[0].headers;
  assert.equal(headers["x-open-compute-worker-id"], "w-real");
  assert.equal(headers["x-open-compute-namespace-resource-id"], "ns-real");
  assert.equal(headers["x-open-compute-binding-id"], props.bindingId);
  assert.equal(
    headers["x-open-compute-resource-spec-generation"],
    String(props.resourceSpecGeneration),
  );
  assert.equal(headers["x-open-compute-version-id"], props.versionId);
  assert.equal(headers["x-open-compute-descriptor-sha256"], props.descriptorSha256);
  assert.equal(headers["content-type"], "application/json");
});

test("wrapBindingBackend rejects unusable backend and bad props", () => {
  assert.throws(
    () => wrapBindingBackend(null, makeProps()),
    (e) => e.stableCode === "BINDING_PROTOCOL_ERROR",
  );
  assert.throws(
    () => wrapBindingBackend({ fetch: "nope" }, makeProps()),
    (e) => e.stableCode === "BINDING_PROTOCOL_ERROR",
  );
  assert.throws(
    () => wrapBindingBackend({}, makeProps({ workerId: "" })),
    (e) => e.stableCode === "BINDING_PROTOCOL_ERROR",
  );
});
