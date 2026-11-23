#!/usr/bin/env node
// Verify the `cloudflare.config.*` worker sandbox end to end.
//
//   node spike/config-sandbox/verify.mjs [--workerd PATH] [--bundler PATH] [--json]
//
// Every case drives the two production helpers, never a copy of the chain:
//
//   scripts/load-cloudflare-config.mjs         (Node loader, the baseline)
//   scripts/load-cloudflare-config-worker.mjs  (of-bundler -> workerd dynamic
//                                               worker -> trusted zod/convert)
//
// It asserts parity where both can evaluate, and fail-closed behaviour
// (typed error, bounded wall clock, no hang) for hostile configs. CI runs
// this in the `real-workerd` job; locally it needs a workerd binary and a
// built of-bundler helper.
//
// Exit 1 means an assertion failed. Missing prerequisites abort with exit 2
// instead of silently passing.

import { spawn } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, "../..");
const FIXTURE = join(REPO, "vrt-local/tests/fixtures/cf-config");
const NODE_HELPER = join(REPO, "scripts/load-cloudflare-config.mjs");
const WORKER_HELPER = join(REPO, "scripts/load-cloudflare-config-worker.mjs");
const COMPAT = "2026-09-17";
// Mirrors CF_CHILD_ENV_ALLOW: both production helpers run with a cleared
// environment in `vrt-local/src/project_cfg.rs`, so the verification harness
// reproduces that instead of comparing against an unfiltered parent env.
const ENV_ALLOW = [
  "PATH",
  "HOME",
  "TMPDIR",
  "TMP",
  "TEMP",
  "LANG",
  "LC_ALL",
  "LC_CTYPE",
  "TZ",
  "USERPROFILE",
  "APPDATA",
  "LOCALAPPDATA",
  "XDG_CACHE_HOME",
  "SYSTEMROOT",
  "SYSTEMDRIVE",
  "WINDIR",
  "COMSPEC",
  "PATHEXT",
  "OF_BUNDLER",
  "VRT_WORKERD_BIN",
  "OF_WORKERD_BIN",
];

function sanitizedEnv(extra = {}) {
  const env = {};
  for (const key of ENV_ALLOW) {
    if (process.env[key] !== undefined) env[key] = process.env[key];
  }
  return { ...env, ...extra };
}

// ── args / environment ─────────────────────────────────────────────────

const argv = process.argv.slice(2);
const opts = {
  json: false,
  workerd: process.env.VRT_WORKERD_BIN || process.env.OF_WORKERD_BIN,
  bundler: process.env.OF_BUNDLER || join(REPO, "of-bundler/target/debug/of-bundler"),
};
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === "--workerd") opts.workerd = argv[++i];
  else if (argv[i] === "--bundler") opts.bundler = argv[++i];
  else if (argv[i] === "--json") opts.json = true;
  else {
    console.error(`unknown argument: ${argv[i]}`);
    process.exit(2);
  }
}
if (!opts.workerd) {
  console.error("workerd required: pass --workerd PATH or set VRT_WORKERD_BIN");
  process.exit(2);
}
try {
  statSync(opts.workerd);
} catch {
  console.error(`workerd not readable: ${opts.workerd}`);
  process.exit(2);
}
try {
  statSync(opts.bundler);
} catch {
  console.error(`of-bundler not readable: ${opts.bundler} (build of-bundler/Cargo.toml)`);
  process.exit(2);
}
try {
  statSync(join(REPO, "node_modules/@cloudflare/config"));
} catch {
  console.error("node_modules/@cloudflare/config missing (run pnpm install)");
  process.exit(2);
}

// ── helpers ────────────────────────────────────────────────────────────

function run(script, args, { env = {}, timeoutMs = 60_000 } = {}) {
  return new Promise((ok) => {
    const started = process.hrtime.bigint();
    const child = spawn(process.execPath, [script, ...args], {
      cwd: REPO,
      stdio: ["ignore", "pipe", "pipe"],
      env: (() => {
        const childEnv = sanitizedEnv();
        for (const [key, value] of Object.entries(env)) {
          if (value === undefined) delete childEnv[key];
          else childEnv[key] = value;
        }
        return childEnv;
      })(),
    });
    const out = [];
    const err = [];
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);
    child.stdout.on("data", (d) => out.push(d));
    child.stderr.on("data", (d) => err.push(d));
    child.on("close", (code) => {
      clearTimeout(timer);
      const elapsedMs = Math.round(Number(process.hrtime.bigint() - started) / 1e6);
      const stderr = Buffer.concat(err).toString("utf8");
      let diagnostic;
      const last = stderr.trim().split("\n").filter(Boolean).pop();
      if (last) {
        try {
          diagnostic = JSON.parse(last);
        } catch {
          // plain text
        }
      }
      ok({
        code,
        timedOut,
        elapsedMs,
        stderr,
        diagnostic,
        stdout: Buffer.concat(out).toString("utf8"),
      });
    });
  });
}

function workerArgs(configPath, extra = []) {
  return [
    configPath,
    ...extra,
    "--bundler",
    opts.bundler,
    "--workerd",
    opts.workerd,
  ];
}

function runWorker(configPath, extra = [], options = {}) {
  return run(WORKER_HELPER, workerArgs(configPath, extra), options);
}

function runNode(configPath, extra = [], options = {}) {
  return run(NODE_HELPER, [configPath, ...extra], options);
}

function parseStdout(result) {
  try {
    return JSON.parse(result.stdout);
  } catch {
    return undefined;
  }
}

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((k) => [k, canonical(value[k])]),
    );
  }
  return value;
}

function diff(a, b, path = "") {
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) {
    return a === b ? [] : [`${path || "<root>"}: ${JSON.stringify(a)} != ${JSON.stringify(b)}`];
  }
  if (Array.isArray(a) !== Array.isArray(b)) return [`${path}: array mismatch`];
  if (Array.isArray(a)) {
    if (a.length !== b.length) return [`${path}: length ${a.length} != ${b.length}`];
    return a.flatMap((v, i) => diff(v, b[i], `${path}[${i}]`));
  }
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  return [...keys].flatMap((k) => diff(a[k], b[k], path ? `${path}.${k}` : k));
}

function makeProject(files) {
  const dir = mkdtempSync(join(tmpdir(), "of-cfg-verify-"));
  for (const [name, body] of Object.entries(files)) {
    const path = join(dir, name);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, body);
  }
  // The Node loader resolves `@cloudflare/config` from the project, so a
  // generated project links the repo install the way a user install does.
  mkdirSync(join(dir, "node_modules/@cloudflare"), { recursive: true });
  symlinkSync(
    join(REPO, "node_modules/@cloudflare/config"),
    join(dir, "node_modules/@cloudflare/config"),
  );
  return dir;
}

/** Assert the Node and worker helpers produced the same RawConfig. */
async function assertParity(runCase, name, configPath, extra = [], env = {}) {
  const worker = await runWorker(configPath, extra, { env });
  const node = await runNode(configPath, extra, { env });
  if (worker.code !== 0) {
    return runCase.fail(`${name}: worker path exited ${worker.code}: ${worker.stderr.trim()}`);
  }
  if (node.code !== 0) {
    return runCase.fail(`${name}: node baseline exited ${node.code}: ${node.stderr.trim()}`);
  }
  const w = parseStdout(worker);
  const n = parseStdout(node);
  const d = diff(canonical(w.raw), canonical(n.raw));
  if (d.length > 0) return runCase.fail(`${name}: RawConfig differs: ${d.join("; ")}`);
  const dd = diff(canonical(w.dependencies ?? []), canonical(n.dependencies ?? []));
  if (dd.length > 0) return runCase.fail(`${name}: dependencies differ: ${dd.join("; ")}`);
  return runCase.pass(`${name}: identical RawConfig (${worker.elapsedMs} ms)`);
}

function expectFailure(runCase, result, { name, code, contains, maxMs }) {
  if (result.timedOut) return runCase.fail(`${name}: helper hung and had to be killed`);
  if (result.code === 0) return runCase.fail(`${name}: expected a failure, got success`);
  if (code && result.diagnostic?.error !== code) {
    return runCase.fail(`${name}: expected ${code}, got ${JSON.stringify(result.diagnostic)}`);
  }
  if (contains && !(result.diagnostic?.message ?? "").includes(contains)) {
    return runCase.fail(`${name}: diagnostic lacks '${contains}': ${result.stderr.trim()}`);
  }
  if (maxMs && result.elapsedMs > maxMs) {
    return runCase.fail(`${name}: took ${result.elapsedMs} ms (limit ${maxMs} ms)`);
  }
  return runCase.pass(`${name}: ${result.diagnostic?.error}${maxMs ? ` in ${result.elapsedMs} ms` : ""}`);
}

// ── cases ──────────────────────────────────────────────────────────────

const cases = [];
const cleanups = [];

async function main() {
  // 1. parity across mode / isPreview branches
  {
    const c = { name: "parity", checks: [] };
    c.pass = (m) => c.checks.push({ ok: true, message: m });
    c.fail = (m) => c.checks.push({ ok: false, message: m });
    await assertParity(c, "mode staging", join(FIXTURE, "cloudflare.config.ts"), ["--mode", "staging"]);
    await assertParity(c, "preview", join(FIXTURE, "cloudflare.config.ts"), ["--preview"]);
    await assertParity(c, "no mode", join(FIXTURE, "cloudflare.config.ts"));
    cases.push(c);
  }

  // 2. nested defineWorker/defineContainer factories
  {
    const dir = makeProject({
      "cloudflare.config.ts": `import { bindings, defineConfig, defineContainer, defineWorker, exports } from "@cloudflare/config";
const container = defineContainer({ name: "svc", image: { dockerfile: "./Dockerfile" } });
const api = defineWorker(() => ({
  name: "nested-worker",
  compatibilityDate: "${COMPAT}",
  env: { FROM_NESTED: bindings.text("yes") },
  exports: { MyContainerDO: exports.durableObject({ storage: "sqlite", container }) },
}));
export default defineConfig({ worker: api, containers: [container] });
`,
    });
    cleanups.push(dir);
    const c = { name: "nested factories", checks: [] };
    c.pass = (m) => c.checks.push({ ok: true, message: m });
    c.fail = (m) => c.checks.push({ ok: false, message: m });
    await assertParity(c, "nested defineWorker + defineContainer", join(dir, "cloudflare.config.ts"));
    const worker = await runWorker(join(dir, "cloudflare.config.ts"));
    const raw = parseStdout(worker)?.raw;
    if (raw?.exports?.MyContainerDO?.container !== "svc") {
      c.fail("nested container reference was not normalized to its name");
    } else {
      c.pass("container reference normalized to 'svc'");
    }
    cases.push(c);
  }

  // 3. process.env injection (same allowlist as the Node child)
  {
    const dir = makeProject({
      "cloudflare.config.ts": `import { bindings, defineConfig } from "@cloudflare/config";
export default defineConfig(() => ({
  worker: {
    name: "env-probe",
    compatibilityDate: "${COMPAT}",
    env: {
      TZ: bindings.text(process.env.TZ ?? "absent"),
      SECRET: bindings.text(process.env.VRT_SANDBOX_SECRET === undefined ? "absent" : "leaked"),
    },
  },
}));
`,
    });
    cleanups.push(dir);
    const c = { name: "env injection", checks: [] };
    c.pass = (m) => c.checks.push({ ok: true, message: m });
    c.fail = (m) => c.checks.push({ ok: false, message: m });
    const env = { TZ: "Asia/Shanghai" };
    await assertParity(c, "allowlisted TZ", join(dir, "cloudflare.config.ts"), [], env);
    // A variable the Rust parent never allowlists must not cross even if it
    // reaches the helper's own environment (defence in depth).
    const worker = await runWorker(join(dir, "cloudflare.config.ts"), [], {
      env: { ...env, VRT_SANDBOX_SECRET: "must-not-cross" },
    });
    const vars = parseStdout(worker)?.raw?.vars;
    if (vars?.TZ !== "Asia/Shanghai") c.fail(`TZ not injected: ${JSON.stringify(vars)}`);
    else c.pass("TZ injected");
    if (vars?.SECRET !== "absent") c.fail(`non-allowlisted env leaked: ${JSON.stringify(vars)}`);
    else c.pass("non-allowlisted env absent");
    cases.push(c);
  }

  // 4. cf-worker import: synthesised specifier only, entry never executes
  {
    const dir = makeProject({
      "cloudflare.config.ts": `import { defineConfig } from "@cloudflare/config";
import * as entrypoint from "./src/index.js" with { type: "cf-worker" };
export default defineConfig({ worker: { name: "entry", compatibilityDate: "${COMPAT}", entrypoint } });
`,
      "src/index.js": 'throw new Error("cf-worker entry must not execute");\n',
    });
    cleanups.push(dir);
    const c = { name: "cf-worker specifier", checks: [] };
    c.pass = (m) => c.checks.push({ ok: true, message: m });
    c.fail = (m) => c.checks.push({ ok: false, message: m });
    await assertParity(c, "entrypoint synthesized without execution", join(dir, "cloudflare.config.ts"));
    const worker = await runWorker(join(dir, "cloudflare.config.ts"));
    if (!parseStdout(worker)?.raw?.main?.endsWith("src/index.js")) {
      c.fail("entrypoint path was not carried into main");
    } else {
      c.pass("entrypoint path carried into main");
    }
    cases.push(c);
  }

  // 5. dependencies include imported project sources
  {
    const dir = makeProject({
      "config-helper.ts": `export const NAME = "from-helper";\n`,
      "cloudflare.config.ts": `import { bindings, defineConfig } from "@cloudflare/config";
import { NAME } from "./config-helper.ts";
export default defineConfig({ worker: { name: NAME, compatibilityDate: "${COMPAT}", env: { X: bindings.text("1") } } });
`,
    });
    cleanups.push(dir);
    const c = { name: "dependencies", checks: [] };
    c.pass = (m) => c.checks.push({ ok: true, message: m });
    c.fail = (m) => c.checks.push({ ok: false, message: m });
    await assertParity(c, "imported helper", join(dir, "cloudflare.config.ts"));
    const deps = parseStdout(await runWorker(join(dir, "cloudflare.config.ts")))?.dependencies ?? [];
    if (!deps.some((d) => d.endsWith("config-helper.ts"))) {
      c.fail(`config-helper.ts missing from dependencies: ${JSON.stringify(deps)}`);
    } else {
      c.pass(`${deps.length} project dependencies reported`);
    }
    cases.push(c);
  }

  // 5b. dynamic import() chunks are shipped and evaluated
  {
    const dir = makeProject({
      "lazy.ts": `export const VALUE = "from-lazy-chunk";\n`,
      "cloudflare.config.ts": `import { bindings, defineConfig } from "@cloudflare/config";
export default defineConfig(async () => {
  const lazy = await import("./lazy.ts");
  return { worker: { name: "lazy", compatibilityDate: "${COMPAT}", env: { V: bindings.text(lazy.VALUE) } } };
});
`,
    });
    cleanups.push(dir);
    const c = { name: "dynamic import chunk", checks: [] };
    c.pass = (m) => c.checks.push({ ok: true, message: m });
    c.fail = (m) => c.checks.push({ ok: false, message: m });
    const configPath = join(dir, "cloudflare.config.ts");
    const worker = await runWorker(configPath);
    const node = await runNode(configPath);
    if (worker.code !== 0) {
      c.fail(`worker path exited ${worker.code}: ${worker.stderr.trim()}`);
    } else if (node.code !== 0) {
      c.fail(`node baseline exited ${node.code}: ${node.stderr.trim()}`);
    } else {
      const d = diff(canonical(parseStdout(worker).raw), canonical(parseStdout(node).raw));
      if (d.length > 0) c.fail(`RawConfig differs: ${d.join("; ")}`);
      else c.pass("lazy chunk evaluated, identical RawConfig");
      const deps = parseStdout(worker).dependencies ?? [];
      if (!deps.some((d) => d.endsWith("lazy.ts"))) {
        c.fail(`lazy.ts missing from dependencies: ${JSON.stringify(deps)}`);
      } else {
        c.pass("dynamically imported source reported");
      }
    }
    cases.push(c);
  }

  // 6-9. hostile configs fail closed
  {
    const fsRead = makeProject({
      "cloudflare.config.ts": `import { readFileSync } from "node:fs";
import { bindings, defineConfig } from "@cloudflare/config";
export default defineConfig(() => ({ worker: { name: "fs", compatibilityDate: "${COMPAT}", env: { LEAK: bindings.text(readFileSync("/etc/passwd", "utf8")) } } }));
`,
    });
    const network = makeProject({
      "cloudflare.config.ts": `import { bindings, defineConfig } from "@cloudflare/config";
export default defineConfig(async () => {
  const res = await fetch("http://169.254.169.254/latest/meta-data/");
  return { worker: { name: "net", compatibilityDate: "${COMPAT}", env: { BODY: bindings.text(await res.text()) } } };
});
`,
    });
    const socket = makeProject({
      "cloudflare.config.ts": `import { connect } from "cloudflare:sockets";
import { bindings, defineConfig } from "@cloudflare/config";
export default defineConfig(async () => {
  const s = connect({ hostname: "169.254.169.254", port: 80 });
  await s.writable.getWriter().write(new TextEncoder().encode("GET / HTTP/1.0\\r\\n\\r\\n"));
  return { worker: { name: "sock", compatibilityDate: "${COMPAT}", env: { X: bindings.text("1") } } };
});
`,
    });
    const oversized = makeProject({
      "cloudflare.config.ts": `import { bindings, defineConfig } from "@cloudflare/config";
const pad = "x".repeat(600 * 1024);
export default defineConfig(() => ({ worker: { name: "big", compatibilityDate: "${COMPAT}", env: { P: bindings.text(pad) } } }));
`,
    });
    const invalid = makeProject({
      "cloudflare.config.ts": `import { defineConfig } from "@cloudflare/config";
export default defineConfig(() => ({ worker: { name: 42 } }));
`,
    });
    cleanups.push(fsRead, network, socket, oversized, invalid);
    const c = { name: "hostile configs", checks: [] };
    c.pass = (m) => c.checks.push({ ok: true, message: m });
    c.fail = (m) => c.checks.push({ ok: false, message: m });
    expectFailure(c, await runWorker(join(fsRead, "cloudflare.config.ts")), {
      name: "host filesystem",
      code: "VRT_CONFIG_LOADER_FAILED",
      contains: "no such file",
    });
    expectFailure(c, await runWorker(join(network, "cloudflare.config.ts")), {
      name: "outbound fetch",
      code: "VRT_CONFIG_LOADER_FAILED",
    });
    expectFailure(c, await runWorker(join(socket, "cloudflare.config.ts")), {
      name: "cloudflare:sockets connect",
      code: "VRT_CONFIG_LOADER_FAILED",
    });
    expectFailure(c, await runWorker(join(oversized, "cloudflare.config.ts")), {
      name: "oversized payload",
      code: "VRT_CONFIG_LOADER_FAILED",
      contains: "exceeds",
    });
    const invalidResult = await runWorker(join(invalid, "cloudflare.config.ts"));
    expectFailure(c, invalidResult, { name: "invalid config", code: "VRT_CONFIG_INVALID" });
    if (!invalidResult.diagnostic?.issues?.some((i) => i.path === "worker.name")) {
      c.fail(`invalid config issue path missing: ${invalidResult.stderr.trim()}`);
    } else {
      c.pass("zod issue path reported by the trusted side");
    }
    cases.push(c);
  }

  // 10. wall-clock deadline: while(true) must fail, never hang
  {
    const loop = makeProject({
      "cloudflare.config.ts": `import { defineConfig } from "@cloudflare/config";
export default defineConfig(() => { while (true) {} });
`,
    });
    cleanups.push(loop);
    const c = { name: "wall-clock timeout", checks: [] };
    c.pass = (m) => c.checks.push({ ok: true, message: m });
    c.fail = (m) => c.checks.push({ ok: false, message: m });
    const result = await runWorker(join(loop, "cloudflare.config.ts"), ["--timeout-ms", "1000"], {
      timeoutMs: 30_000,
    });
    expectFailure(c, result, {
      name: "while(true)",
      code: "VRT_CONFIG_LOADER_TIMEOUT",
      maxMs: 15_000,
    });
    cases.push(c);
  }

  // 11. missing workerd fails closed instead of silently using Node
  {
    const c = { name: "missing workerd", checks: [] };
    c.pass = (m) => c.checks.push({ ok: true, message: m });
    c.fail = (m) => c.checks.push({ ok: false, message: m });
    const result = await run(
      WORKER_HELPER,
      [join(FIXTURE, "cloudflare.config.ts"), "--bundler", opts.bundler],
      { env: { VRT_WORKERD_BIN: undefined, OF_WORKERD_BIN: undefined }, timeoutMs: 20_000 },
    );
    expectFailure(c, result, {
      name: "no workerd binary",
      code: "VRT_CONFIG_LOADER_FAILED",
      contains: "workerd",
    });
    cases.push(c);
  }

  // ── report ───────────────────────────────────────────────────────────
  let failed = 0;
  for (const c of cases) {
    const ok = c.checks.every((check) => check.ok);
    if (!ok) failed++;
    if (opts.json) continue;
    console.log(`\n=== ${c.name}: ${ok ? "PASS" : "FAIL"}`);
    for (const check of c.checks) {
      console.log(`  ${check.ok ? "ok  " : "FAIL"} ${check.message}`);
    }
  }
  if (opts.json) {
    console.log(JSON.stringify({ ok: failed === 0, cases }, null, 2));
  } else {
    console.log(`\n${failed === 0 ? "RESULT: PASS" : `RESULT: FAIL (${failed} case(s))`}`);
  }
  process.exitCode = failed === 0 ? 0 : 1;
}

main()
  .catch((error) => {
    console.error(`fatal: ${error?.stack ?? error}`);
    process.exitCode = 1;
  })
  .finally(() => {
    for (const dir of cleanups) rmSync(dir, { recursive: true, force: true });
  });
