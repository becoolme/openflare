// Real-boundary R2 test: a tenant worker's native `r2Bucket` binding is wired
// to the vendored R2Transport entrypoint inside a real workerd process. The
// node-based r2.test.mjs calls transport methods directly and never crosses
// the workerd native R2 wire, so bugs in the native adapter (option parsing,
// response shape, delete framing) are only reachable here.
//
// Skipped when no workerd binary is available: set WORKERD_BIN, or put
// `workerd` on PATH. Environments without workerd keep the node-only coverage.
import test from "node:test";
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(fileURLToPath(new URL("../..", import.meta.url)));

function findWorkerd() {
  const candidates = [process.env.WORKERD_BIN, "workerd"].filter(Boolean);
  for (const bin of candidates) {
    const probe = spawnSync(bin, ["--version"], { encoding: "utf8" });
    if (probe.status === 0) return bin;
  }
  return null;
}

const WORKERD = findWorkerd();

const BACKEND = `
const store = new Map();
const enc = new TextEncoder();
const dec = new TextDecoder();
const meta = (key, o) => ({
  key, version: "v1", size: o.bytes.byteLength, etag: "e" + o.bytes.byteLength,
  httpEtag: '"e"', uploaded: 1700000000000, checksums: {}, storageClass: "Standard",
  httpMetadata: o.http, customMetadata: o.custom,
});
const fail = (status = 400) => new Response(null, {
  status, headers: { "x-open-compute-error-code": "BINDING_PROTOCOL_ERROR" },
});
// Mirrors the provider's strict (deny_unknown_fields) request bodies.
const strict = (o, keys) => {
  for (const k of Object.keys(o)) if (!keys.includes(k)) throw new Error("unknown " + k);
};
export default {
  async fetch(req) {
    const op = new URL(req.url).pathname.split("/").pop();
    const raw = new Uint8Array(await req.arrayBuffer());
    const json = (v) => new Response(JSON.stringify(v));
    try {
      if (op === "put") {
        const n = new DataView(raw.buffer, raw.byteOffset).getUint32(0);
        const h = JSON.parse(dec.decode(raw.subarray(4, 4 + n)));
        strict(h, ["key", "options"]);
        strict(h.options, ["httpMetadata", "customMetadata", "onlyIf", "checksum", "storageClass", "ssecKey"]);
        const o = { bytes: raw.subarray(4 + n), http: h.options.httpMetadata, custom: h.options.customMetadata };
        store.set(h.key, o);
        return json(meta(h.key, o));
      }
      const b = JSON.parse(dec.decode(raw));
      if (op === "get") {
        strict(b, ["key", "options"]);
        const o = store.get(b.key);
        if (!o) return new Response(null, { status: 204 });
        const hd = enc.encode(JSON.stringify({ meta: meta(b.key, o), hasBody: true }));
        const out = new Uint8Array(4 + hd.length + o.bytes.length);
        new DataView(out.buffer).setUint32(0, hd.length);
        out.set(hd, 4);
        out.set(o.bytes, 4 + hd.length);
        return new Response(out);
      }
      if (op === "head") {
        strict(b, ["key"]);
        const o = store.get(b.key);
        return o ? json(meta(b.key, o)) : new Response(null, { status: 204 });
      }
      if (op === "delete") {
        strict(b, ["keys"]);
        if (b.keys.length < 1 || b.keys.length > 1000) return fail();
        for (const k of b.keys) store.delete(k);
        return new Response(null, { status: 204 });
      }
      if (op === "list") {
        return json({
          objects: [...store].filter(([k]) => k.startsWith(b.prefix ?? "")).map(([k, o]) => meta(k, o)),
          truncated: false, delimitedPrefixes: [],
        });
      }
    } catch {
      return fail();
    }
    return fail(404);
  },
};
`;

// Tenant-side scenarios run inside workerd against env.BUCKET; they are
// serialized into the tenant module source (workerd forbids eval).
const SCENARIOS = {
  async putWithMetadata(env) {
    const r = await env.BUCKET.put("p1", "hello", {
      httpMetadata: { contentType: "text/plain" },
      customMetadata: { m: "x1" },
    });
    return { key: r.key, size: r.size, ct: r.httpMetadata.contentType, cm: r.customMetadata };
  },
  async putPlainAndHeaders(env) {
    const a = await env.BUCKET.put("p2", "abc");
    const b = await env.BUCKET.put("p3", "abc", {
      httpMetadata: new Headers({ "content-type": "text/css" }),
    });
    return [a.size, b.httpMetadata.contentType];
  },
  async getBody(env) {
    await env.BUCKET.put("g1", '{"a":1}', {
      httpMetadata: { contentType: "application/json" },
      customMetadata: { m: "x" },
    });
    const o = await env.BUCKET.get("g1");
    const text = await o.text();
    const json = await (await env.BUCKET.get("g1")).json();
    const buf = new Uint8Array(await (await env.BUCKET.get("g1")).arrayBuffer());
    const blob = await (await env.BUCKET.get("g1")).blob();
    const streamed = await new Response((await env.BUCKET.get("g1")).body).text();
    return {
      text, json, len: buf.length, blobSize: blob.size, streamed, size: o.size,
      cm: o.customMetadata, ct: o.httpMetadata.contentType, key: o.key, bodyUsed: o.bodyUsed,
    };
  },
  async missing(env) {
    return [await env.BUCKET.get("nope"), await env.BUCKET.head("nope")];
  },
  async deletes(env) {
    for (const k of ["d1", "d2", "d3"]) await env.BUCKET.put(k, "x");
    await env.BUCKET.delete("d1");
    const afterOne = (await env.BUCKET.list({ prefix: "d" })).objects.map((o) => o.key);
    await env.BUCKET.delete(["d2", "d3"]);
    const afterMany = (await env.BUCKET.list({ prefix: "d" })).objects.map((o) => o.key);
    return { afterOne, afterMany };
  },
  async invalidOptions(env) {
    return env.BUCKET.put("bad", "x", { httpMetadata: 5 }).then(
      () => "resolved",
      (e) => String(e.message),
    );
  },
};

const TENANT = `
const S = {
${Object.entries(SCENARIOS)
  .map(([, f]) => `  ${f.toString()},`) // method shorthand carries its own name
  .join("\n")}
};
export default {
  async fetch(req, env) {
    const name = await req.text();
    try {
      return Response.json({ ok: true, result: await S[name](env) });
    } catch (e) {
      return Response.json({ ok: false, name: e?.constructor?.name, message: String(e?.message ?? e) });
    }
  },
};
`;

function capnpConfig(dir, port) {
  const manifest = JSON.parse(readFileSync(join(ROOT, "facade/manifest.json"), "utf8"));
  const modules = manifest.workers["facade-r2"].modules
    .map((m) => {
      // workerd resolves embed paths relative to the config file.
      const target = join(dir, "mods", m.name);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, readFileSync(join(ROOT, m.dist)));
      return `(name = "${m.name}", esModule = embed "mods/${m.name}")`;
    })
    .join(",\n");
  const props = JSON.stringify({
    bindingId: "b1",
    versionId: "v1",
    workerId: "w",
    instanceId: "i",
    descriptorSha256: "a".repeat(64),
    namespaceResourceId: "n",
    resourceSpecGeneration: 1,
    permissions: { read: true, write: true },
  });
  return `using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "tenant.js", esModule = embed "tenant.js")],
      compatibilityDate = "2025-01-01",
      bindings = [(name = "BUCKET", r2Bucket = (name = "r2", entrypoint = "R2Transport",
        props = (json = ${JSON.stringify(props)})))])),
    (name = "r2", worker = (
      modules = [${modules}],
      compatibilityDate = "2025-01-01",
      bindings = [(name = "BINDING_BACKEND", service = "backend")])),
    (name = "backend", worker = (
      modules = [(name = "backend.js", esModule = embed "backend.js")],
      compatibilityDate = "2025-01-01")),
  ],
  sockets = [(name = "http", address = "127.0.0.1:${port}", http = (), service = "main")],
);
`;
}

let dir;
let proc;
let port;
let output = "";

async function run(name) {
  const response = await fetch(`http://127.0.0.1:${port}/`, {
    method: "POST",
    body: name,
  });
  return response.json();
}

test.before(async () => {
  if (!WORKERD) return;
  dir = mkdtempSync(join(tmpdir(), "r2-workerd-"));
  port = 20000 + Math.floor(Math.random() * 20000);
  writeFileSync(join(dir, "tenant.js"), TENANT);
  writeFileSync(join(dir, "backend.js"), BACKEND);
  writeFileSync(join(dir, "cfg.capnp"), capnpConfig(dir, port));
  proc = spawn(WORKERD, ["serve", "--experimental", join(dir, "cfg.capnp")], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  proc.stdout.on("data", (d) => (output += d));
  proc.stderr.on("data", (d) => (output += d));
  const deadline = Date.now() + 15000;
  for (;;) {
    try {
      await fetch(`http://127.0.0.1:${port}/`, { method: "POST", body: "missing" });
      return;
    } catch {
      if (Date.now() > deadline || proc.exitCode !== null) {
        throw new Error(`workerd did not start: ${output}`);
      }
      await new Promise((r) => setTimeout(r, 100));
    }
  }
});

test.after(() => {
  proc?.kill("SIGKILL");
  if (dir) rmSync(dir, { recursive: true, force: true });
});

const opts = { skip: WORKERD ? false : "workerd binary not found (set WORKERD_BIN)" };

test("put accepts httpMetadata and customMetadata through the native wire", opts, async () => {
  assert.deepEqual(await run("putWithMetadata"), {
    ok: true,
    result: { key: "p1", size: 5, ct: "text/plain", cm: { m: "x1" } },
  });
});

test("put with no options, and with Headers httpMetadata", opts, async () => {
  assert.deepEqual(await run("putPlainAndHeaders"), { ok: true, result: [3, "text/css"] });
});

test("get returns a body object with working accessors", opts, async () => {
  assert.deepEqual(await run("getBody"), {
    ok: true,
    result: {
      text: '{"a":1}', json: { a: 1 }, len: 7, blobSize: 7, streamed: '{"a":1}',
      size: 7, cm: { m: "x" }, ct: "application/json", key: "g1", bodyUsed: true,
    },
  });
});

test("get and head of a missing key are null", opts, async () => {
  assert.deepEqual(await run("missing"), { ok: true, result: [null, null] });
});

test("delete(key) and delete([keys]) remove objects", opts, async () => {
  assert.deepEqual(await run("deletes"), {
    ok: true,
    result: { afterOne: ["d2", "d3"], afterMany: [] },
  });
});

test("genuinely invalid put options still fail closed", opts, async () => {
  const out = await run("invalidOptions");
  assert.equal(out.ok, true);
  assert.notEqual(out.result, "resolved");
});
