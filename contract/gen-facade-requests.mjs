#!/usr/bin/env node
// Generate contract/facade-requests.json — the cross-component protocol
// fixture replayed by vrt-provider/tests/contract_replay.rs.
//
// What it captures: the REAL requests the published facade bytes send to
// binding-backend for KV put/get/list/delete and R2 put/head/get/list/
// delete, each with a decoded `expect` summary — the semantic result the
// real facade produced from a canned backend reply. The Rust replay
// asserts the provider accepts every request AND yields the same decoded
// summary, so both directions are covered: facade → wire (recorded here)
// and wire → facade (decoded `expect` matched against provider output).
//
// Requests are ordered for self-seeding: put precedes the ops that read
// the key/object it stores, so the replay needs no fixture-only state.
//
// Usage: node gen-facade-requests.mjs [--check]
import "../facade/test/shims/register.mjs";
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  bytes,
  facadeModule,
  jsonResponse,
  kvs1,
  kvJson,
  r2Frame,
  r2Meta,
  streamOf,
} from "../facade/test/helpers.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT_FILE = resolve(HERE, "facade-requests.json");

// Props mirror what vrt-config renders into capnp service props
// (`<workerId>~<bindingName>`, permissions from compute.desired).
const KV_PROPS = {
  bindingId: "w-contract-1~KV",
  versionId: "v-contract-1",
  workerId: "w-contract-1",
  instanceId: "tenant-contract",
  descriptorSha256: "c".repeat(64),
  namespaceResourceId: "kv-contract-ns",
  resourceSpecGeneration: 11,
  permissions: { read: true, write: true },
};
const R2_PROPS = {
  ...KV_PROPS,
  bindingId: "w-contract-1~MEDIA",
  namespaceResourceId: "bucket-contract-1",
};

// Headers the replay injects itself: x-open-compute-request-id is a fresh
// UUID per call — recorded value is meaningless, so it is excluded and the
// replay side sets a fixed canonical one.
const EXCLUDED_HEADERS = new Set(["x-open-compute-request-id"]);

function backend(reply) {
  const requests = [];
  const fetcher = {
    async fetch(input, init = {}) {
      const body =
        init.body == null
          ? null
          : new Uint8Array(await new Response(init.body).arrayBuffer());
      const headers = {};
      new Headers(init.headers).forEach((value, key) => {
        if (!EXCLUDED_HEADERS.has(key.toLowerCase())) headers[key] = value;
      });
      const record = { url: String(input), method: init.method ?? "GET", headers, body };
      requests.push(record);
      return reply(record);
    },
  };
  return { requests, env: { BINDING_BACKEND: fetcher } };
}

const { KVNamespace } = await import(facadeModule("facade-kv.js"));
const { R2Transport } = await import(facadeModule("facade-r2.js"));

async function run(scenario) {
  const { requests, env } = backend(scenario.reply);
  const impl =
    scenario.worker === "facade-kv"
      ? new KVNamespace({ props: scenario.props }, env)
      : new R2Transport({ props: scenario.props }, env);
  const decoded = await scenario.call(impl);
  if (requests.length !== 1) {
    throw new Error(`${scenario.label}: expected 1 request, got ${requests.length}`);
  }
  const req = requests[0];
  const url = new URL(req.url);
  return {
    label: scenario.label,
    worker: scenario.worker,
    method: req.method,
    path: url.pathname,
    headers: req.headers,
    bodyBase64: req.body ? Buffer.from(req.body).toString("base64") : "",
    expect: { status: scenario.status, decoded: decoded ?? null },
  };
}

const R2_META = (size) =>
  r2Meta({ key: "contract-object", size, etag: "e".repeat(32), httpEtag: `"${"e".repeat(32)}"` });

const SCENARIOS = [
  {
    label: "kv-put",
    worker: "facade-kv",
    props: KV_PROPS,
    call: (ns) => ns.put("contract-key", "contract-kv-value"),
    reply: () => new Response(null, { status: 200 }),
    status: 200,
  },
  {
    label: "kv-get",
    worker: "facade-kv",
    props: KV_PROPS,
    call: (ns) => ns.get("contract-key"),
    reply: () => kvs1(bytes("contract-kv-value")),
    status: 200,
  },
  {
    label: "kv-list",
    worker: "facade-kv",
    props: KV_PROPS,
    call: (ns) => ns.list({ prefix: "contract", limit: 10 }),
    reply: () =>
      kvJson({
        keys: [{ name: "contract-key", expiration: null, metadata: null }],
        list_complete: true,
        cursor: null,
      }),
    status: 200,
  },
  {
    label: "kv-delete",
    worker: "facade-kv",
    props: KV_PROPS,
    call: (ns) => ns.delete("contract-key"),
    reply: () => new Response(null, { status: 200 }),
    status: 200,
  },
  {
    label: "r2-put",
    worker: "facade-r2",
    props: R2_PROPS,
    call: (bucket) =>
      bucket.put("contract-object", streamOf(bytes("contract-r2-body")), {}),
    reply: () => jsonResponse(R2_META(16)),
    status: 200,
  },
  {
    label: "r2-head",
    worker: "facade-r2",
    props: R2_PROPS,
    call: (bucket) => bucket.head("contract-object"),
    reply: () => jsonResponse(R2_META(16)),
    status: 200,
  },
  {
    label: "r2-get",
    worker: "facade-r2",
    props: R2_PROPS,
    call: async (bucket) => {
      const result = await bucket.get("contract-object", {});
      const text = await new Response(result.body).text();
      return { key: result.meta.key, size: result.meta.size, text };
    },
    reply: () => r2Frame(R2_META(16), bytes("contract-r2-body")),
    status: 200,
  },
  {
    label: "r2-list",
    worker: "facade-r2",
    props: R2_PROPS,
    call: async (bucket) => {
      const result = await bucket.list({ prefix: "contract" });
      return {
        truncated: result.truncated,
        keys: result.objects.map((o) => o.key),
        delimitedPrefixes: result.delimitedPrefixes,
      };
    },
    reply: () =>
      jsonResponse({
        objects: [R2_META(16)],
        truncated: false,
        cursor: null,
        delimitedPrefixes: [],
      }),
    status: 200,
  },
  {
    label: "r2-delete",
    worker: "facade-r2",
    props: R2_PROPS,
    call: (bucket) => bucket.delete(["contract-object"]),
    reply: () => new Response(null, { status: 204 }),
    status: 204,
  },
];

const requests = [];
for (const scenario of SCENARIOS) {
  try {
    requests.push(await run(scenario));
  } catch (error) {
    console.error(`scenario ${scenario.label} failed:`, error);
    process.exit(1);
  }
}

const output =
  JSON.stringify(
    {
      schemaVersion: 1,
      generatedBy: "contract/gen-facade-requests.mjs",
      note: "x-open-compute-request-id is per-call; the replay injects a fixed canonical UUID.",
      props: { kv: KV_PROPS, r2: R2_PROPS },
      requests,
    },
    null,
    2,
  ) + "\n";

if (process.argv.includes("--check")) {
  const current = existsSync(OUT_FILE) ? readFileSync(OUT_FILE, "utf8") : "";
  if (current !== output) {
    console.error("gen-facade-requests --check: fixture is stale; rerun the generator");
    process.exit(1);
  }
  console.log("gen-facade-requests --check: up to date");
} else {
  writeFileSync(OUT_FILE, output);
  console.log(`gen-facade-requests: wrote ${requests.length} requests`);
}
