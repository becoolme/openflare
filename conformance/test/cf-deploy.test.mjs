import test from "node:test";
import assert from "node:assert/strict";
import {
  stripJsonc,
  provisionPlan,
  applyProvisioned,
  deployedHostname,
} from "../cf-deploy.mjs";

test("stripJsonc removes comments and trailing commas, keeps strings", () => {
  const src = `{
    // line comment
    "a": "http://x/y", /* url-ish string stays */
    "b": [1, 2,],
    "c": "escaped \\\\" // real comment
  }`;
  assert.deepEqual(JSON.parse(stripJsonc(src)), {
    a: "http://x/y",
    b: [1, 2],
    c: 'escaped \\',
  });
});

test("provisionPlan extracts every provisionable binding", () => {
  const plan = provisionPlan({
    kv_namespaces: [{ binding: "KV", id: "conf-kv" }],
    r2_buckets: [{ binding: "B", bucket_name: "conf-b" }],
    d1_databases: [{ binding: "DB", database_name: "conf-db", database_id: "x" }],
    queues: {
      producers: [{ binding: "Q", queue: "conf-q" }],
      consumers: [{ queue: "conf-q" }, { queue: "conf-q2" }],
    },
  });
  assert.deepEqual(plan.kv, [{ binding: "KV", title: "conf-kv" }]);
  assert.deepEqual(plan.r2, [{ binding: "B", name: "conf-b" }]);
  assert.deepEqual(plan.d1, [{ binding: "DB", name: "conf-db" }]);
  assert.deepEqual(plan.queues, ["conf-q", "conf-q2"]);
});

test("applyProvisioned drops routes, sets workers_dev, injects ids", () => {
  const patched = applyProvisioned(
    {
      name: "conf-x",
      routes: ["conf-x.vrt.local/*"],
      kv_namespaces: [{ binding: "KV", id: "conf-kv" }],
      d1_databases: [{ binding: "DB", database_name: "conf-db", database_id: "placeholder" }],
      vars: { A: "1" },
    },
    { kv: { KV: "real-kv-id" }, d1: { DB: "real-d1-uuid" } },
  );
  assert.equal(patched.routes, undefined);
  assert.equal(patched.workers_dev, true);
  assert.equal(patched.kv_namespaces[0].id, "real-kv-id");
  assert.equal(patched.d1_databases[0].database_id, "real-d1-uuid");
  assert.equal(patched.vars.A, "1");
});

test("deployedHostname parses workers.dev URL from deploy output", () => {
  const out = "Uploaded conf-kv-basic\nDeployed conf-kv-basic triggers\n  https://conf-kv-basic.acct.workers.dev\n";
  assert.equal(deployedHostname(out, "conf-kv-basic"), "conf-kv-basic.acct.workers.dev");
  const sub = process.env.CF_SUBDOMAIN;
  process.env.CF_SUBDOMAIN = "acct";
  try {
    assert.equal(deployedHostname("no url here", "conf-x"), "conf-x.acct.workers.dev");
  } finally {
    if (sub === undefined) delete process.env.CF_SUBDOMAIN;
    else process.env.CF_SUBDOMAIN = sub;
  }
  delete process.env.CF_SUBDOMAIN;
  assert.throws(() => deployedHostname("no url", "conf-x"), /CF_SUBDOMAIN/);
});
