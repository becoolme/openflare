import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  compareReports,
  readReferenceManifest,
  referenceSupport,
  renderComparison,
  renderReferenceConfig,
  stripJsonc,
  supportsBundledModules,
} from "../reference.mjs";

function caseDir(manifest, files = { "worker.js": "export default { fetch() {} };\n" }) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "conf-ref-test-"));
  fs.writeFileSync(path.join(dir, "wrangler.jsonc"), JSON.stringify(manifest, null, 2));
  for (const [name, body] of Object.entries(files)) fs.writeFileSync(path.join(dir, name), body);
  fs.writeFileSync(
    path.join(dir, "case.json"),
    JSON.stringify({ schemaVersion: 1, id: "t", binding: "kv", steps: [] }),
  );
  return dir;
}

const entry = (dir, id = "t") => ({
  id,
  binding: "kv",
  dir,
  ok: true,
  case: { schemaVersion: 1, id, binding: "kv", title: id, manifest: "wrangler.jsonc", steps: [] },
});

test("stripJsonc removes comments and trailing commas but keeps strings", () => {
  const src = `// leading
  {
    "a": "// not a comment, /* nor this */",
    "b": 1, // trailing comment
    /* block
       comment */
    "c": [1, 2,],
  }
  `;
  const doc = JSON.parse(stripJsonc(src));
  assert.deepEqual(doc, { a: "// not a comment, /* nor this */", b: 1, c: [1, 2] });
  // Escaped quotes must not end the string early.
  assert.equal(JSON.parse(stripJsonc('{"a":"x\\"//y"}')).a, 'x"//y');
});

test("referenceSupport names every key it cannot map", () => {
  const dir = caseDir({
    name: "c",
    main: "worker.js",
    compatibility_date: "2024-09-25",
    queues: { producers: [] },
    durable_objects: { bindings: [] },
  });
  const support = referenceSupport(readReferenceManifest(dir));
  assert.equal(support.ok, false);
  assert.match(support.reason, /durable_objects/);
  assert.match(support.reason, /queues/);
});

test("vrt.toml-only cases report why they cannot run as reference", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "conf-ref-toml-"));
  fs.writeFileSync(path.join(dir, "vrt.toml"), 'name = "c"\n');
  assert.equal(referenceSupport(readReferenceManifest(dir)).ok, false);
  assert.match(referenceSupport(readReferenceManifest(dir)).reason, /wrangler manifest/);
});

test("kv renders the bundled kv worker wiring wrangler uses", () => {
  const dir = caseDir({
    name: "conf-kv",
    main: "worker.js",
    compatibility_date: "2024-09-25",
    kv_namespaces: [{ binding: "KV", id: "ns-1" }],
  });
  const r = renderReferenceConfig({ entry: entry(dir), port: 1234, stateDir: "/tmp/state" });
  assert.equal(r.ok, true);
  assert.match(r.text, /modules = \[\(name = "kv\.js", bundled = "\/workerd\/kv\.js"\)\]/);
  assert.match(r.text, /\(name = "SHARD", durableObjectNamespace = \(className = "KVNamespace"\)\)/);
  assert.match(r.text, /durableObjectNamespaces = \[\(className = "KVNamespace", uniqueKey = "KVNamespace", enableSql = true\)\]/);
  assert.match(r.text, /durableObjectStorage = \(localDisk = "conf-kv-kv-storage"\)/);
  assert.match(r.text, /\(name = "BLOBS", service = "conf-kv-kv-blobs"\)/);
  assert.match(r.text, /\(name = "KV", kvNamespace = \(name = "conf-kv-kv", props = \(json = "\{\\"namespace\\":\\"ns-1\\"\}"\)\)\)/);
  // The disk service backing the DO storage is emitted, writable.
  assert.match(r.text, /\(name = "conf-kv-kv-storage", disk = \(path = "[^"]+", writable = true\)\)/);
  assert.match(r.text, /sockets = \[\(name = "http", address = "127\.0\.0\.1:1234", service = "conf-kv"\)\]/);
  // No facade, no binding-backend: the reference stack is self-contained.
  assert.ok(!r.text.includes("facade-"));
});

test("d1 renders the wrapped d1-api binding over a bundled DO", () => {
  const dir = caseDir({
    name: "conf-d1",
    main: "worker.js",
    compatibility_date: "2024-09-25",
    d1_databases: [{ binding: "DB", database_id: "db-1", database_name: "d" }],
  });
  const r = renderReferenceConfig({ entry: entry(dir), port: 1, stateDir: "/tmp/s" });
  assert.equal(r.ok, true);
  assert.match(r.text, /bundled = "\/workerd\/d1\.js"/);
  assert.match(r.text, /\(className = "D1Database", uniqueKey = "D1Database", enableSql = true\)/);
  assert.match(r.text, /moduleName = "cloudflare-internal:d1-api"/);
  assert.match(r.text, /props = \(json = "\{\\"database\\":\\"db-1\\"\}"\)/);
});

test("r2 and ratelimits render their bundled services", () => {
  const dir = caseDir({
    name: "conf-r2",
    main: "worker.js",
    compatibility_date: "2024-09-25",
    r2_buckets: [{ binding: "BUCKET", bucket_name: "b" }],
    ratelimits: [{ name: "RL", namespace_id: "1001", simple: { limit: 10, period: 60 } }],
  });
  const r = renderReferenceConfig({ entry: entry(dir), port: 1, stateDir: "/tmp/s" });
  assert.equal(r.ok, true);
  assert.match(r.text, /bundled = "\/workerd\/r2\.js"/);
  assert.match(r.text, /\(name = "conf-r2-r2-blobs", bucket = \(path = "r2", localDisk = "conf-r2-r2-storage"\)\)/);
  assert.match(r.text, /bundled = "\/workerd\/ratelimit\.js"/);
  assert.match(r.text, /entrypoint = "RateLimit"/);
  assert.match(r.text, /\\"limit\\":10/);
  assert.match(r.text, /\\"period\\":60/);
});

test("the case's main module is emitted first", () => {
  const dir = caseDir(
    { name: "c", main: "z.js", compatibility_date: "2024-09-25" },
    { "z.js": "export default {};", "a.js": "export const a = 1;" },
  );
  const r = renderReferenceConfig({ entry: entry(dir), port: 1, stateDir: "/tmp/s" });
  const modules = r.text.slice(r.text.indexOf("modules = ["), r.text.indexOf("],\n      bindings"));
  assert.ok(modules.indexOf('"z.js"') < modules.indexOf('"a.js"'));
});

test("missing main module fails instead of rendering a broken config", () => {
  const dir = caseDir({ name: "c", main: "nope.js", compatibility_date: "2024-09-25" });
  const r = renderReferenceConfig({ entry: entry(dir), port: 1, stateDir: "/tmp/s" });
  assert.equal(r.ok, false);
  assert.match(r.reason, /main module/);
});

test("supportsBundledModules reads the schema, not a version string", () => {
  assert.equal(supportsBundledModules('bundled @11 :Text;'), true);
  // The 2025-04 workerd shipped by npm predates the bundled modules.
  assert.equal(supportsBundledModules('struct Module { union { esModule @1 :Text; } }'), false);
  assert.equal(supportsBundledModules(null), false);
});

test("compareReports separates VRT-only from bundled-only", () => {
  const vrt = {
    cases: [
      {
        id: "c1",
        status: "failed",
        steps: [
          { name: "s-pass", status: "passed" },
          { name: "s-fail", status: "failed" },
          { name: "s-bothfail", status: "failed" },
        ],
      },
      { id: "c2", status: "skipped", steps: [] },
    ],
  };
  const bundled = {
    cases: [
      {
        id: "c1",
        status: "passed",
        steps: [
          { name: "s-pass", status: "passed" },
          { name: "s-fail", status: "passed" },
          { name: "s-bothfail", status: "failed" },
        ],
      },
    ],
  };
  const c = compareReports(vrt, bundled);
  assert.deepEqual(c.agreements.map((r) => r.step), ["s-pass"]);
  assert.deepEqual(c.bundledOnly.map((r) => r.step), ["s-fail"]);
  assert.deepEqual(c.bothFail.map((r) => r.step), ["s-bothfail"]);
  assert.equal(c.vrtOnly.length, 0);
  const md = renderComparison(c);
  assert.match(md, /only bundled passes/);
  assert.match(md, /s-fail/);
});
