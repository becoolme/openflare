#!/usr/bin/env node
// Generate facade/dist/ + facade/manifest.json (schemaVersion 2).
//
// Pipeline (deterministic):
//   1. walk the static relative-import graph of each system worker's TypeScript
//      entry module (vendored open-compute uses `.js` specifiers for `.ts`
//      files, so logical module names are repo-relative paths with `.ts`→`.js`)
//   2. transpile every module with Node's built-in
//      `module.stripTypeScriptTypes(src, { mode: "transform" })` — the bytes
//      workerd embeds. Real workerd does NOT strip TypeScript; publishing
//      source bytes would fail to load (`kv/native-adapter.ts` even uses a
//      parameter property). Emitted files keep the module's relative path
//      under facade/dist/ (the dist root mirrors the repo root), so `.js`
//      specifiers resolve unchanged and need no rewriting.
//   3. re-scan the emitted dist graph and prove the published closure is
//      identical to the source closure (same modules, same externals).
//   4. record per module: logical `name`, `dist`/`source` repo-relative paths,
//      `sha256` over the DIST bytes (what workerd embeds), `sourceSha256`
//      over the TypeScript source (provenance only).
//
// Rules enforced (fail closed):
//   - every import specifier is relative AND resolves to a file inside the
//     repo (workerd embeds modules by name; nothing may escape the graph)
//   - bare specifiers are only workerd-provided externals
//     (`cloudflare:*`, `cloudflare-internal:*`, `node:*`) — recorded, never
//     embedded
//   - no dynamic import()
//   - `import type`/`export type` statements are erased, not resolved
//   - the declared entrypoint must be exported by the entry module
//
// Usage: node gen-manifest.mjs [--check]
//   default : clean-rebuild dist/ and rewrite manifest.json
//   --check : rebuild everything in memory and byte-compare against the
//             committed dist/ + manifest.json; exit 1 on any drift
//             (missing/extra/changed file), exit 0 when identical.
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { stripTypeScriptTypes } from "node:module";

const HERE = dirname(fileURLToPath(import.meta.url));
const FACADE_DIR = resolve(HERE, "..");
const ROOT = resolve(HERE, "../..");
const DIST_DIR = resolve(FACADE_DIR, "dist");
const OUT_FILE = resolve(FACADE_DIR, "manifest.json");
const NODE_MAJOR = process.versions.node.split(".")[0];

const WORKERS = [
  { serviceName: "facade-kv", entrypoint: "KVNamespace", entry: "facade-kv.ts", needsBindingBackend: true },
  { serviceName: "facade-r2", entrypoint: "R2Transport", entry: "facade-r2.ts", needsBindingBackend: true },
  { serviceName: "facade-assets", entrypoint: "AssetTransport", entry: "facade-assets.ts", needsBindingBackend: true },
  { serviceName: "facade-pg", entrypoint: "PgTransport", entry: "facade-pg.ts", needsBindingBackend: true },
  { serviceName: "facade-d1", entrypoint: "D1Facade", entry: "facade-d1.ts", needsBindingBackend: true },
  { serviceName: "facade-queue", entrypoint: "QueueFacade", entry: "facade-queue.ts", needsBindingBackend: true },
  // K2 durable event log — inner `fetcher` of the k2 wrapped binding
  // (produce-only: POST /send → provider → vrt-sql K2Backend).
  { serviceName: "facade-k2", entrypoint: "K2Facade", entry: "facade-k2.ts", needsBindingBackend: true },
  { serviceName: "facade-email", entrypoint: "EmailFacade", entry: "facade-email.ts", needsBindingBackend: true },
  // MIG-16: in-process Rate Limiting binding — pure RPC entrypoint, no
  // binding-backend (state is local to the workerd process).
  { serviceName: "facade-ratelimit", entrypoint: "RateLimiter", entry: "facade-ratelimit.ts", needsBindingBackend: false },
  // T37b: consumer delivery bridge — pure RPC proxy (CONSUMER
  // service binding → tenant worker); no binding-backend.
  { serviceName: "facade-qconsumer", entrypoint: "QueueConsumerFacade", entry: "facade-qconsumer.ts", needsBindingBackend: false },
  { serviceName: "facade-router", entrypoint: "AssetsRouter", entry: "facade-router.ts", needsBindingBackend: false },
  // T6: tenant egress gate. No binding-backend — it talks only to the
  // outside world through `outbound-network`, never to platform APIs.
  { serviceName: "vrt-egress", entrypoint: "EgressProxy", entry: "egress/egress.ts", needsBindingBackend: false },
  // Deny-all globalOutbound target for system workers that must never
  // reach a network at all (facades only talk to BINDING_BACKEND).
  { serviceName: "vrt-no-network", entrypoint: "DenyAll", entry: "egress/deny.ts", needsBindingBackend: false },
];

const EXTERNAL_PREFIXES = ["cloudflare:", "cloudflare-internal:", "node:"];
const MODULE_EXTS = [".ts", ".js", ".mjs"];

function fail(file, message) {
  throw new Error(`gen-manifest: ${file}: ${message}`);
}

// Strip line/block comments while preserving string and template contents,
// so import specifiers are only collected from real code positions.
function stripComments(source, file) {
  const frames = [{ mode: "code", braces: 0 }];
  let out = "";
  let i = 0;
  const top = () => frames[frames.length - 1];
  while (i < source.length) {
    const c = source[i];
    const n = source[i + 1];
    const f = top();
    if (f.mode === "line") {
      if (c === "\n") { out += c; frames.pop(); }
      i++;
      continue;
    }
    if (f.mode === "block") {
      if (c === "*" && n === "/") { frames.pop(); i += 2; continue; }
      if (c === "\n") out += c;
      i++;
      continue;
    }
    if (f.mode === "sq" || f.mode === "dq") {
      out += c;
      if (c === "\\") { out += n ?? ""; i += 2; continue; }
      if ((f.mode === "sq" && c === "'") || (f.mode === "dq" && c === '"')) frames.pop();
      i++;
      continue;
    }
    if (f.mode === "tpl") {
      out += c;
      if (c === "\\") { out += n ?? ""; i += 2; continue; }
      if (c === "`") { frames.pop(); i++; continue; }
      if (c === "$" && n === "{") { out += n; frames.push({ mode: "code", braces: 0 }); i += 2; continue; }
      i++;
      continue;
    }
    // code
    if (c === "/" && n === "/") { frames.push({ mode: "line" }); i += 2; continue; }
    if (c === "/" && n === "*") { frames.push({ mode: "block" }); i += 2; continue; }
    if (c === "'") { frames.push({ mode: "sq" }); out += c; i++; continue; }
    if (c === '"') { frames.push({ mode: "dq" }); out += c; i++; continue; }
    if (c === "`") { frames.push({ mode: "tpl" }); out += c; i++; continue; }
    if (c === "{") { f.braces++; out += c; i++; continue; }
    if (c === "}") {
      out += c;
      if (f.braces === 0 && frames.length > 1) frames.pop();
      else f.braces--;
      i++;
      continue;
    }
    out += c;
    i++;
  }
  if (frames.length !== 1) fail(file, "unterminated comment/string/template");
  return out;
}

const TYPE_IMPORT_RE = /\b(?:import|export)\s+type\s[^'";]*?(?:\bfrom\s*["'][^"']+["']|;?)/g;
const STATIC_SPEC_RE = /(?:import|export)\s[^'";]*?\bfrom\s*["']([^"']+)["']|\bimport\s*["']([^"']+)["']/g;
const DYNAMIC_IMPORT_RE = /\bimport\s*\(/;

function importsOf(file, source) {
  const stripped = stripComments(source, file);
  if (DYNAMIC_IMPORT_RE.test(stripped)) fail(file, "dynamic import() is not allowed");
  const code = stripped.replace(TYPE_IMPORT_RE, "");
  const specs = [];
  for (const match of code.matchAll(STATIC_SPEC_RE)) {
    specs.push(match[1] ?? match[2]);
  }
  return specs;
}

// Resolve an import specifier against a concrete file. `container` is the
// directory that must contain every resolved file (repo root for sources,
// dist root for emitted files); `tries` maps a `.js` specifier to the
// candidate module files for that container.
function resolveSpecifier(spec, fromFile, container, tries) {
  if (!spec.startsWith("./") && !spec.startsWith("../")) {
    if (EXTERNAL_PREFIXES.some((p) => spec.startsWith(p))) {
      return { external: spec };
    }
    fail(fromFile, `non-relative specifier ${JSON.stringify(spec)} is not a declared external`);
  }
  const candidate = resolve(dirname(fromFile), spec);
  const inside = relative(container, candidate);
  if (inside.startsWith("..") || inside === "" || inside.includes(`..${sep}`)) {
    fail(fromFile, `specifier ${JSON.stringify(spec)} escapes the module root`);
  }
  for (const t of tries(candidate)) {
    if (existsSync(t) && MODULE_EXTS.some((e) => t.endsWith(e))) {
      if (t.endsWith(".d.ts")) {
        fail(fromFile, `specifier ${JSON.stringify(spec)} resolves to a declaration file`);
      }
      return { file: t };
    }
  }
  fail(fromFile, `specifier ${JSON.stringify(spec)} does not resolve inside the module graph`);
}

const SOURCE_TRIES = (candidate) => {
  const tries = [candidate];
  if (candidate.endsWith(".js")) {
    tries.push(candidate.slice(0, -3) + ".ts", candidate.slice(0, -3) + ".mjs");
  }
  return tries;
};
// Dist files are already `.js`; a specifier resolves only to the literal file.
const DIST_TRIES = (candidate) => [candidate];

function moduleName(repoRelative) {
  return repoRelative.endsWith(".ts") ? repoRelative.slice(0, -3) + ".js" : repoRelative;
}

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function assertEntrypoint(source, file, entrypoint) {
  const stripped = stripComments(source, file);
  const direct = new RegExp(`\\bexport\\s+(?:async\\s+)?(?:class|const|function|let|var)\\s+${entrypoint}\\b`);
  const braced = new RegExp(`\\bexport\\s*\\{[^}]*\\b${entrypoint}\\b[^}]*\\}`);
  const def = new RegExp(`\\bexport\\s+default\\s+${entrypoint}\\b`);
  if (!direct.test(stripped) && !braced.test(stripped) && !def.test(stripped)) {
    fail(file, `entrypoint ${entrypoint} is not exported by the entry module`);
  }
}

// Walk the static import graph from `entryFile` inside `container`,
// resolving specifiers via `tries`. Returns {files, externals}.
function walk(entryFile, container, tries) {
  const files = new Set();
  const externals = new Set();
  const queue = [entryFile];
  while (queue.length) {
    const file = queue.shift();
    if (files.has(file)) continue;
    files.add(file);
    const source = readFileSync(file, "utf8");
    for (const spec of importsOf(file, source)) {
      const resolved = resolveSpecifier(spec, file, container, tries);
      if (resolved.external) externals.add(resolved.external);
      else queue.push(resolved.file);
    }
  }
  return { files, externals };
}

// Transpile one source file to its published bytes.
function publish(source, file) {
  if (file.endsWith(".ts")) {
    const out = stripTypeScriptTypes(source, { mode: "transform" });
    return out.endsWith("\n") ? out : `${out}\n`;
  }
  return source;
}

function build() {
  const externals = new Map();
  const workers = {};
  const emitted = new Map(); // absolute dist path -> bytes
  for (const worker of WORKERS) {
    const entryFile = resolve(FACADE_DIR, worker.entry);
    if (!existsSync(entryFile)) fail(entryFile, "entry module missing");
    const { files, externals: workerExternals } = walk(entryFile, ROOT, SOURCE_TRIES);
    for (const spec of workerExternals) {
      externals.set(spec, "workerd-provided external");
    }
    assertEntrypoint(readFileSync(entryFile, "utf8"), entryFile, worker.entrypoint);

    const modules = [];
    for (const file of files) {
      const rel = relative(ROOT, file).split(sep).join("/");
      const name = moduleName(rel);
      const source = readFileSync(file, "utf8");
      const published = publish(source, file);
      const distAbs = resolve(DIST_DIR, name);
      emitted.set(distAbs, published);
      modules.push({
        name,
        dist: relative(ROOT, distAbs).split(sep).join("/"),
        source: rel,
        sourceSha256: sha256Hex(source),
        sha256: sha256Hex(published),
      });
    }
    modules.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    if (new Set(modules.map((m) => m.name)).size !== modules.length) {
      fail(entryFile, "duplicate logical module name");
    }
    workers[worker.serviceName] = {
      entrypoint: worker.entrypoint,
      needsBindingBackend: worker.needsBindingBackend,
      mainModule: moduleName(relative(ROOT, entryFile).split(sep).join("/")),
      modules,
    };
  }

  // Re-scan the emitted graph: published specifiers must close over exactly
  // the emitted set with no new externals and no `.ts`/`.d.ts` references.
  for (const worker of WORKERS) {
    const entryName = workers[worker.serviceName].mainModule;
    const seen = new Set();
    const queue = [resolve(DIST_DIR, entryName)];
    while (queue.length) {
      const file = queue.shift();
      if (seen.has(file)) continue;
      seen.add(file);
      const published = emitted.get(file);
      if (published === undefined) {
        fail(file, "dist graph references a module outside the emitted set");
      }
      for (const spec of importsOf(file, published)) {
        const candidate = resolve(dirname(file), spec);
        if (spec.startsWith("./") || spec.startsWith("../")) {
          const inside = relative(DIST_DIR, candidate);
          if (inside.startsWith("..") || inside === "" || inside.includes(`..${sep}`)) {
            fail(file, `published specifier ${JSON.stringify(spec)} escapes dist/`);
          }
          if (!emitted.has(candidate)) {
            fail(file, `published specifier ${JSON.stringify(spec)} has no emitted module`);
          }
          queue.push(candidate);
        } else if (!externals.has(spec)) {
          fail(file, `published specifier ${JSON.stringify(spec)} is not a declared external`);
        }
      }
    }
    const expected = new Set(
      workers[worker.serviceName].modules.map((m) => resolve(DIST_DIR, m.name)),
    );
    for (const file of expected) {
      if (!seen.has(file)) fail(file, "emitted module unreachable from the worker entry");
    }
  }

  const manifest =
    JSON.stringify(
      {
        schemaVersion: 2,
        generatedBy: "facade/tools/gen-manifest.mjs",
        nodeVersion: NODE_MAJOR,
        externals: Object.fromEntries(
          [...externals.keys()].sort().map((k) => [k, externals.get(k)]),
        ),
        workers,
      },
      null,
      2,
    ) + "\n";
  return { manifest, emitted };
}

// Every file currently inside dist/, as dist-relative posix paths.
function diskDistFiles() {
  const out = [];
  const visit = (dir) => {
    if (!existsSync(dir)) return;
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const abs = resolve(dir, entry.name);
      if (entry.isDirectory()) visit(abs);
      else if (entry.isFile()) out.push(abs);
    }
  };
  visit(DIST_DIR);
  return out;
}

const { manifest, emitted } = build();

if (process.argv.includes("--check")) {
  const drift = [];
  const current = existsSync(OUT_FILE) ? readFileSync(OUT_FILE, "utf8") : "";
  if (current !== manifest) drift.push("manifest.json differs from regenerated output");
  const expected = new Set(emitted.keys());
  for (const [file, bytes] of emitted) {
    const onDisk = existsSync(file) ? readFileSync(file, "utf8") : null;
    if (onDisk !== bytes) {
      drift.push(`${relative(FACADE_DIR, file)} differs (or is missing)`);
    }
  }
  for (const file of diskDistFiles()) {
    if (!expected.has(file)) {
      drift.push(`${relative(FACADE_DIR, file)} is an extra file not produced by the build`);
    }
  }
  if (drift.length) {
    for (const line of drift) console.error(`gen-manifest --check: ${line}`);
    console.error("gen-manifest --check: drift detected; rerun gen-manifest.mjs");
    process.exit(1);
  }
  console.log("gen-manifest --check: manifest.json and dist/ are up to date");
} else {
  rmSync(DIST_DIR, { recursive: true, force: true });
  for (const [file, bytes] of [...emitted.entries()].sort()) {
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, bytes);
  }
  writeFileSync(OUT_FILE, manifest);
  console.log(
    `gen-manifest: wrote ${relative(ROOT, OUT_FILE)} and ${emitted.size} dist modules (node ${NODE_MAJOR})`,
  );
}
