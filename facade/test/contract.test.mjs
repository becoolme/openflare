import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import test from "node:test";

const CONTRACT = new URL("../../contract/", import.meta.url).pathname;
const FIXTURE = new URL("../../contract/facade-requests.json", import.meta.url).pathname;

test("contract: facade-requests.json is fresh (gen --check gate)", () => {
  execFileSync(
    process.execPath,
    [new URL("../../contract/gen-facade-requests.mjs", import.meta.url).pathname, "--check"],
    { cwd: CONTRACT, stdio: "pipe" },
  );
});

test("contract: fixture covers the agreed op set and records expectations", () => {
  const fixture = JSON.parse(readFileSync(FIXTURE, "utf8"));
  assert.equal(fixture.schemaVersion, 1);
  const labels = fixture.requests.map((r) => r.label);
  assert.deepEqual(labels, [
    "kv-put", "kv-get", "kv-list", "kv-delete",
    "r2-put", "r2-head", "r2-get", "r2-list", "r2-delete",
  ]);
  for (const r of fixture.requests) {
    assert.ok(r.method === "POST", r.label);
    assert.ok(r.path.startsWith("/internal/bindings/v1/"), r.label);
    assert.match(r.headers["x-open-compute-binding-id"], /^[A-Za-z0-9_.:-]+~[A-Za-z_$][A-Za-z0-9_$]*$/, r.label);
    assert.equal(typeof r.bodyBase64, "string", r.label);
    assert.equal(typeof r.expect.status, "number", r.label);
    assert.ok("decoded" in r.expect, r.label);
  }
  assert.match(fixture.note, /x-open-compute-request-id/);
  for (const r of fixture.requests) {
    assert.ok(!("x-open-compute-request-id" in r.headers), r.label);
  }
});
