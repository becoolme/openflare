import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const FACADE_DIR = resolve(HERE, "..");
const ROOT = resolve(FACADE_DIR, "..");
const MANIFEST = JSON.parse(
  readFileSync(resolve(FACADE_DIR, "manifest.json"), "utf8"),
);

function sha256(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

const FACADES = {
  "facade-kv": "KVNamespace",
  "facade-r2": "R2Transport",
  "facade-assets": "AssetTransport",
  "facade-pg": "PgTransport",
};

// System workers without the binding-backend service binding (T6).
const STANDALONE = {
  "vrt-egress": "EgressProxy",
  "vrt-no-network": "DenyAll",
};

test("schema carries build provenance", () => {
  assert.equal(MANIFEST.schemaVersion, 2);
  assert.match(MANIFEST.generatedBy, /gen-manifest\.mjs$/);
  // Pinned Node major: stripTypeScriptTypes output is only guaranteed
  // deterministic within one major; --check detects drift if regenerated
  // under another.
  assert.match(MANIFEST.nodeVersion, /^\d+$/);
  assert.equal(MANIFEST.nodeVersion, process.versions.node.split(".")[0]);
});

test("every facade worker declares a closed module graph", () => {
  for (const [service, entrypoint] of Object.entries({
    ...FACADES,
    ...STANDALONE,
  })) {
    const worker = MANIFEST.workers[service];
    assert.ok(worker, `missing ${service}`);
    assert.equal(worker.entrypoint, entrypoint);
    assert.equal(
      worker.needsBindingBackend,
      service in FACADES,
      `${service} needsBindingBackend`,
    );
    assert.ok(worker.modules.length > 0);
    const names = worker.modules.map((m) => m.name);
    assert.ok(names.includes(worker.mainModule));
    assert.equal(new Set(names).size, names.length);
  }
  assert.ok(MANIFEST.externals["cloudflare:workers"]);
});

test("module digests address dist bytes, sources tracked separately", () => {
  let sawDistDistinctFromSource = false;
  for (const worker of Object.values(MANIFEST.workers)) {
    for (const module of worker.modules) {
      const distPath = resolve(ROOT, module.dist);
      const sourcePath = resolve(ROOT, module.source);
      // name is the logical specifier-facing path; dist holds its bytes.
      assert.equal(module.dist, `facade/dist/${module.name}`);
      assert.equal(
        module.name,
        module.source.replace(/\.ts$/, ".js"),
      );
      assert.equal(sha256(distPath), module.sha256, `${module.name} dist digest`);
      assert.equal(sha256(sourcePath), module.sourceSha256, `${module.name} source digest`);
      if (module.sha256 !== module.sourceSha256) sawDistDistinctFromSource = true;
    }
  }
  // At least props.js must differ — it carries TypeScript-only syntax
  // (interfaces/annotations) that transform erases, proving digests really
  // address different bytes.
  assert.ok(sawDistDistinctFromSource);
});

test("all published modules load as plain JavaScript under node", async () => {
  // Dynamic-import of every dist module is the load-level proof that the
  // emitted bytes carry no TypeScript residue (the same check workerd's
  // esModule load would perform).
  const failures = [];
  for (const worker of Object.values(MANIFEST.workers)) {
    for (const module of worker.modules) {
      const url = pathToFileURL(resolve(ROOT, module.dist)).href;
      try {
        await import(url);
      } catch (error) {
        failures.push(`${module.name}: ${error.message}`);
      }
    }
  }
  assert.deepEqual(failures, []);
});

test("entrypoints are exported by their dist entry modules", async () => {
  for (const [service, entrypoint] of Object.entries({
    ...FACADES,
    ...STANDALONE,
  })) {
    const url = pathToFileURL(
      resolve(FACADE_DIR, "dist", MANIFEST.workers[service].mainModule),
    ).href;
    const mod = await import(url);
    assert.equal(typeof mod[entrypoint], "function", `${service} entrypoint`);
    assert.equal(mod.default, mod[entrypoint]);
  }
});

test("gen-manifest --check passes (no build drift)", () => {
  const out = execFileSync(
    process.execPath,
    [resolve(FACADE_DIR, "tools/gen-manifest.mjs"), "--check"],
    { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
  );
  assert.match(out, /up to date/);
});
