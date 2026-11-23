#!/usr/bin/env node
// Load `cloudflare.config.ts` / `.js` in a workerd dynamic worker.
//
//   node scripts/load-cloudflare-config-worker.mjs <configPath>
//        [--mode M] [--preview] [--timeout-ms N]
//        [--bundler PATH] [--workerd PATH]
//
// Same stdout/stderr contract as `scripts/load-cloudflare-config.mjs`, so
// `vrt-local/src/project_cfg.rs` consumes either path with one parser:
//
// stdout (exit 0):
//   {"raw": <wrangler RawConfig>, "dependencies": ["<abs path>", ...]}
//
// stderr (single-line JSON) + non-zero exit:
//   2  the config did not parse against CloudflareConfigSchema
//      ({"error":"VRT_CONFIG_INVALID","message":…,"issues":[…]})
//   3  bundling, sandbox startup, evaluation or conversion failed
//      ({"error":"VRT_CONFIG_LOADER_FAILED"|"VRT_CONFIG_LOADER_TIMEOUT",
//        "message":…,"phase":"bundle"|"startup"|"eval"|"convert"})
//   64 invalid command line (never seen by vrt itself)
//
// Chain: of-bundler (alias @cloudflare/config → Workers-safe shim, and every
// `with { type: "cf-worker" }` specifier → a module exporting the path) →
// workerd `workerLoader` dynamic worker (own isolate: no host FS, no
// outbound network) → the wrapper mirrors @cloudflare/config's definition
// resolver (nested defineWorker/defineContainer factories) → JSON → this
// process validates with CloudflareConfigSchema and converts on the trusted
// side.
//
// Isolation/timeout contract:
//   * the dynamic worker gets `limits.cpuMs` as a backstop, but upstream
//     workerd does not enforce it and the pinned fork terminates the isolate
//     without completing the RPC — a caller-visible wall-clock deadline is
//     the only response-time bound;
//   * the deadline below (--timeout-ms, default 5 s) aborts the request and
//     SIGKILLs the workerd process *group*; there is no retry.
//   * the readiness probe is tagged with a per-run nonce, so an ephemeral
//     port stolen between binding and workerd's listen can only fail
//     startup (fail closed) — never route a config into another sandbox.
//
// The config never runs in this process. This script only executes fixed
// code (bundler, workerd, zod, convertToWranglerConfig).

import { spawn } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  writeFileSync,
  writeSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SHIM = join(HERE, "shims/cloudflare-config-public.mjs");
const DEFAULT_TIMEOUT_MS = 5_000;
const MIN_TIMEOUT_MS = 100;
const MAX_TIMEOUT_MS = 30_000;
const MAX_CONFIG_BYTES = 256 * 1024;
const MAX_BUNDLE_BYTES = 8 * 1024 * 1024;
const MAX_PAYLOAD_BYTES = 512 * 1024;
const MAX_ISSUES = 20;
const MAX_MESSAGE_CHARS = 500;
const MAX_LOG_BYTES = 16 * 1024;
const BUNDLE_TIMEOUT_MS = 30_000;
const STARTUP_TIMEOUT_MS = 15_000;
const COMPAT_DATE = "2026-09-17";
const CONFIG_EXTENSIONS = new Set([".ts", ".mts", ".cts", ".js", ".mjs", ".cjs"]);
// Mirrors CF_CHILD_ENV_ALLOW in vrt-local/src/project_cfg.rs: the Node path
// hands the config exactly these variables. The worker gets the same map as
// a frozen `process.env`, so both paths see one environment contract.
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
];

// ── stderr contract ────────────────────────────────────────────────────

function writeOut(text) {
  writeSync(1, text);
}

function writeErr(text) {
  writeSync(2, text);
}

function truncate(message, max = MAX_MESSAGE_CHARS) {
  const text = String(message ?? "").replace(/\s+/g, " ").trim();
  const chars = Array.from(text);
  return chars.length > max ? `${chars.slice(0, max).join("")}...` : text;
}

function emitError(error, message, extra = {}) {
  writeErr(`${JSON.stringify({ error, message: truncate(message), ...extra })}\n`);
  process.exit(error === "VRT_CONFIG_INVALID" ? 2 : 3);
}

/** Last non-empty line of a bounded workerd log, for crash diagnostics. */
function lastLogLine(text) {
  const lines = String(text ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.length === 0 ? "" : lines[lines.length - 1];
}

function usage(message) {
  emitError("VRT_CONFIG_LOADER_USAGE", message);
}

// ── args ───────────────────────────────────────────────────────────────

function parseArgs(argv) {
  const out = {
    configPath: undefined,
    mode: undefined,
    preview: false,
    timeoutMs: envInt("OF_CF_SANDBOX_TIMEOUT_MS", DEFAULT_TIMEOUT_MS),
    bundler: process.env.OF_BUNDLER || undefined,
    workerd: process.env.VRT_WORKERD_BIN || process.env.OF_WORKERD_BIN || undefined,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--mode") out.mode = requireValue(argv, ++i, "--mode");
    else if (a === "--preview") out.preview = true;
    else if (a === "--timeout-ms") out.timeoutMs = Number(requireValue(argv, ++i, "--timeout-ms"));
    else if (a === "--bundler") out.bundler = requireValue(argv, ++i, "--bundler");
    else if (a === "--workerd") out.workerd = requireValue(argv, ++i, "--workerd");
    else if (a.startsWith("--")) usage(`unknown option ${a}`);
    else if (out.configPath === undefined) out.configPath = a;
    else usage("exactly one config path is accepted");
  }
  if (out.configPath === undefined) usage("config path is required");
  if (!Number.isInteger(out.timeoutMs) || out.timeoutMs < MIN_TIMEOUT_MS || out.timeoutMs > MAX_TIMEOUT_MS) {
    usage(`--timeout-ms must be ${MIN_TIMEOUT_MS}..=${MAX_TIMEOUT_MS}`);
  }
  return out;
}

function requireValue(argv, i, flag) {
  const v = argv[i];
  if (v === undefined || v === "" || v.startsWith("--")) usage(`${flag} requires a value`);
  return v;
}

function envInt(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const n = Number(raw);
  return Number.isInteger(n) ? n : fallback;
}

// ── small helpers ──────────────────────────────────────────────────────

function ms(started) {
  return Math.round(Number(process.hrtime.bigint() - started) / 1e6);
}

function freePort() {
  return new Promise((ok, fail) => {
    const srv = createServer();
    srv.on("error", fail);
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => ok(port));
    });
  });
}

/** Readiness probe; the nonce proves the port belongs to *this* workerd. */
async function waitReady(port, deadline, nonce) {
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/`, {
        method: "GET",
        signal: AbortSignal.timeout(500),
      });
      if (res.ok && (await res.text()) === nonce) return true;
    } catch {
      // not listening yet
    }
    await new Promise((r) => setTimeout(r, 50));
  }
  return false;
}

/** Run a helper with a bounded deadline; returns captured streams. */
function run(cmd, args, { input, timeoutMs, detached = true, cwd } = {}) {
  return new Promise((ok) => {
    const child = spawn(cmd, args, {
      cwd,
      stdio: ["pipe", "pipe", "pipe"],
      detached,
    });
    const out = [];
    const err = [];
    let captured = 0;
    let truncated = false;
    let timedOut = false;
    const collect = (sink) => (d) => {
      if (captured >= MAX_LOG_BYTES) {
        truncated = true;
        return;
      }
      const room = Math.min(d.length, MAX_LOG_BYTES - captured);
      sink.push(d.subarray(0, room));
      captured += room;
      if (room < d.length) truncated = true;
    };
    child.stdout.on("data", collect(out));
    child.stderr.on("data", collect(err));
    const timer = setTimeout(() => {
      timedOut = true;
      killTree(child);
    }, timeoutMs);
    const finish = (code) => {
      clearTimeout(timer);
      ok({
        code,
        timedOut,
        truncated,
        stdout: Buffer.concat(out).toString("utf8"),
        stderr: Buffer.concat(err).toString("utf8"),
      });
    };
    child.on("error", (e) => {
      clearTimeout(timer);
      ok({ code: -1, timedOut: false, truncated, stdout: "", stderr: String(e) });
    });
    child.on("close", (code) => finish(code));
    if (input !== undefined) child.stdin.end(input);
    else child.stdin.end();
  });
}

/** SIGKILL a process group (Unix) and reap the direct child. */
function killTree(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  if (process.platform !== "win32" && child.pid) {
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch {
      // group already gone
    }
  }
  try {
    child.kill("SIGKILL");
  } catch {
    // already exited
  }
}

// ── step 1: bundle ─────────────────────────────────────────────────────

// A `with { type: "cf-worker" }` import is a *specifier*, not a module: the
// Node loader replaces it with a synthetic module whose default export is
// the path. The sandbox does the same with generated shims; the referenced
// entry module is never bundled or executed. Only the literal attribute
// form is supported — anything else fails closed instead of silently
// bundling (and thus including) the entry module.
function cfWorkerAliases(source, projectDir, workDir) {
  const aliases = {};
  const shims = [];
  const patterns = [
    /\bfrom\s*(['"])([^'"\n]+)\1\s*with\s*\{([^}]*)\}/g,
    /\bimport\s*(['"])([^'"\n]+)\1\s*with\s*\{([^}]*)\}/g,
  ];
  const seen = new Set();
  let matched = 0;
  for (const re of patterns) {
    let m;
    while ((m = re.exec(source)) !== null) {
      const spec = m[2];
      if (!/["']?type["']?\s*:\s*["']cf-worker["']/.test(m[3])) continue;
      matched++;
      if (seen.has(spec)) continue;
      seen.add(spec);
      const abs = resolveSpecifier(spec, projectDir);
      const shim = join(workDir, `cf-worker-${shims.length}.mjs`);
      writeFileSync(shim, `export default ${JSON.stringify(abs)};\n`);
      // One alias key per spelling: relative specifiers resolve against the
      // importer, bare ones against the package resolver; `$` = exact.
      aliases[`${spec}$`] = shim;
      shims.push(spec);
    }
  }
  // Every `cf-worker` attribute must have produced a shim. Computed or
  // otherwise unsupported forms fail closed instead of silently bundling
  // (and possibly executing) the entry module.
  const occurrences = (source.match(/type\s*:\s*["']cf-worker["']/g) ?? []).length;
  if (occurrences > matched) {
    throw Object.assign(new Error(`${occurrences - matched} cf-worker import(s) use an unsupported form`), {
      code: "VRT_CONFIG_SANDBOX_CF_WORKER_IMPORT",
    });
  }
  return aliases;
}

function resolveSpecifier(spec, projectDir) {
  try {
    if (spec.startsWith(".") || spec.startsWith("/")) {
      return realpathSync(spec.startsWith("/") ? spec : resolve(projectDir, spec));
    }
    const requireFromProject = createRequire(join(projectDir, "noop.js"));
    return realpathSync(requireFromProject.resolve(spec));
  } catch (e) {
    throw Object.assign(new Error(`cf-worker specifier cannot be resolved (${e?.code ?? e?.message ?? "error"})`), {
      code: "VRT_CONFIG_SANDBOX_CF_WORKER_IMPORT",
    });
  }
}

async function bundle({ projectDir, configFile, bundler, workDir }) {
  const source = readFileSync(join(projectDir, configFile), "utf8");
  let alias;
  try {
    alias = {
      "@cloudflare/config$": SHIM,
      ...cfWorkerAliases(source, projectDir, workDir),
    };
  } catch (e) {
    return { ok: false, phase: "bundle", error: e.message, code: e.code };
  }
  const spec = {
    root: projectDir,
    entry: configFile,
    main_dir: "",
    rule_prefix: "",
    rule_globs: [],
    opts: { nodejs_compat: false, alias },
  };
  const started = process.hrtime.bigint();
  const res = await run(bundler, [], {
    input: JSON.stringify(spec),
    timeoutMs: BUNDLE_TIMEOUT_MS,
  });
  const tookMs = ms(started);
  if (res.timedOut) {
    return { ok: false, tookMs, phase: "bundle", error: `of-bundler exceeded ${BUNDLE_TIMEOUT_MS} ms` };
  }
  if (res.code !== 0) {
    return {
      ok: false,
      tookMs,
      phase: "bundle",
      error: `of-bundler exited ${res.code}: ${truncate(res.stderr || res.stdout, 300)}`,
    };
  }
  let outcome;
  try {
    outcome = JSON.parse(res.stdout);
  } catch {
    return { ok: false, tookMs, phase: "bundle", error: "of-bundler printed invalid JSON" };
  }
  if (outcome.err) return { ok: false, tookMs, phase: "bundle", error: truncate(outcome.err, 300) };
  const bundled = outcome.ok;
  if (!bundled || typeof bundled.main_source !== "string") {
    return { ok: false, tookMs, phase: "bundle", error: "of-bundler returned no main module" };
  }
  if (Array.isArray(bundled.externals) && bundled.externals.length > 0) {
    return {
      ok: false,
      tookMs,
      phase: "bundle",
      error: "non-code imports are not supported in a cloudflare.config sandbox",
    };
  }
  const chunkList = Array.isArray(bundled.chunks) ? bundled.chunks : [];
  const bundleBytes =
    bundled.main_source.length +
    chunkList.reduce((sum, chunk) => sum + (typeof chunk?.[1] === "string" ? chunk[1].length : 0), 0);
  if (bundleBytes > MAX_BUNDLE_BYTES) {
    return {
      ok: false,
      tookMs,
      phase: "bundle",
      error: `config bundle exceeds ${MAX_BUNDLE_BYTES} bytes`,
    };
  }
  const survivingAttribute = [bundled.main_source, ...chunkList.map((chunk) => chunk?.[1])]
    .some((code) => typeof code === "string" && /type\s*:\s*["']cf-worker["']/.test(code));
  if (survivingAttribute) {
    return {
      ok: false,
      tookMs,
      phase: "bundle",
      error: "a cf-worker import attribute survived bundling (unsupported form)",
    };
  }
  const dependencies = Array.isArray(bundled.dependencies)
    ? bundled.dependencies.filter((d) => typeof d === "string")
    : [];
  // Report the project's own config sources, like the Node loader does:
  // package code and the generated cf-worker shims are not user
  // dependencies for cache/reload purposes. Files imported from a sibling
  // directory stay in the list (the Node loader tracks them too).
  const workDirReal = realpathSync(workDir);
  const deps = [
    ...new Set(
      dependencies.filter(
        (d) =>
          !d.includes("/node_modules/") &&
          !d.startsWith(`${workDirReal}/`) &&
          !d.startsWith(`${workDir}/`),
      ),
    ),
  ].sort();
  return {
    ok: true,
    tookMs,
    source: bundled.main_source,
    chunks: Array.isArray(bundled.chunks) ? bundled.chunks : [],
    dependencies: deps,
  };
}

// ── step 2: dynamic worker evaluation ──────────────────────────────────

const WRAPPER = `import "./sandbox-env.js";
import configExport from "./cloudflare.config.js";
import { WorkerEntrypoint } from "cloudflare:workers";

// Mirrors @cloudflare/config's resolveAndParseConfig() normalization: every
// definition may be a value or a factory, references are resolved to names,
// a cf-worker entrypoint namespace collapses to its specifier string, and
// container references are checked against the containers array. Zod
// validation stays on the trusted side (this output is untrusted JSON).
function isRecord(value) {
  return typeof value === "object" && value !== null;
}

const CROSS_WORKER_BINDING_TYPES = new Set(["durable-object", "worker", "workflow"]);

function createDefinitionResolver(ctx) {
  const cache = new Map();
  return (input) => {
    if (cache.has(input)) return cache.get(input);
    const resolved = Promise.resolve().then(() => (typeof input === "function" ? input(ctx) : input));
    cache.set(input, resolved);
    return resolved;
  };
}

function normalizeConfigReference(reference, resolved) {
  return isRecord(resolved) && typeof resolved.name === "string" ? resolved.name : reference;
}

async function normalizeWorkerEntrypoint(resolved) {
  if (!isRecord(resolved) || !isRecord(resolved.entrypoint) || !("default" in resolved.entrypoint)) {
    return resolved;
  }
  return { ...resolved, entrypoint: resolved.entrypoint.default };
}

async function normalizeWorkerEnv(resolved, resolveDefinition) {
  if (!isRecord(resolved) || !isRecord(resolved.env)) return resolved;
  const env = { ...resolved.env };
  for (const [name, binding] of Object.entries(env)) {
    if (binding === false || binding === null || binding === undefined) {
      delete env[name];
      continue;
    }
    if (
      !isRecord(binding) ||
      typeof binding.type !== "string" ||
      !CROSS_WORKER_BINDING_TYPES.has(binding.type) ||
      typeof binding.worker === "string"
    ) {
      continue;
    }
    const target = await resolveDefinition(binding.worker);
    env[name] = { ...binding, worker: normalizeConfigReference(binding.worker, target) };
  }
  return { ...resolved, env };
}

async function normalizeContainerReferences(resolved, resolveDefinition, containers) {
  const issues = [];
  if (!isRecord(resolved) || !isRecord(resolved.exports)) return { value: resolved, issues };
  const exports = { ...resolved.exports };
  for (const [exportName, workerExport] of Object.entries(exports)) {
    if (!isRecord(workerExport) || workerExport.type !== "durable-object" || workerExport.container === undefined) {
      continue;
    }
    const reference = workerExport.container;
    if (typeof reference === "string") {
      issues.push({
        code: "custom",
        path: ["exports", exportName, "container"],
        message:
          "Container provided as a string. Reference a Container definition instead, for example \`container: myContainer\`.",
      });
      exports[exportName] = { ...workerExport, container: undefined };
      continue;
    }
    const target = await resolveDefinition(reference);
    if (isRecord(target) && typeof target.name === "string" && !containers.includes(target)) {
      issues.push({
        code: "custom",
        path: ["exports", exportName, "container"],
        message: "The referenced Container is not included in the \`containers\` array.",
      });
      exports[exportName] = { ...workerExport, container: undefined };
      continue;
    }
    exports[exportName] = {
      ...workerExport,
      container: normalizeConfigReference(reference, target),
    };
  }
  return { value: { ...resolved, exports }, issues };
}

async function normalizeWorkerConfig(resolved, resolveDefinition, containers) {
  return normalizeContainerReferences(
    await normalizeWorkerEnv(await normalizeWorkerEntrypoint(resolved), resolveDefinition),
    resolveDefinition,
    containers,
  );
}

async function resolveConfig(input, ctx) {
  const resolveDefinition = createDefinitionResolver(ctx);
  const resolved = await resolveDefinition(input);
  if (!isRecord(resolved)) return { value: resolved, issues: [] };
  const containerInputs = Array.isArray(resolved.containers) ? resolved.containers : [];
  const containers = await Promise.all(containerInputs.map((c) => resolveDefinition(c)));
  const worker =
    resolved.worker === undefined
      ? { value: undefined, issues: [] }
      : await normalizeWorkerConfig(await resolveDefinition(resolved.worker), resolveDefinition, containers);
  return {
    value: {
      ...resolved,
      ...(resolved.worker === undefined ? {} : { worker: worker.value }),
      ...(Array.isArray(resolved.containers) ? { containers } : {}),
    },
    issues: worker.issues.map((issue) => ({
      ...issue,
      path: ["worker", ...(Array.isArray(issue.path) ? issue.path : [])],
    })),
  };
}

function jsonError(error) {
  return new Response(
    JSON.stringify({ ok: false, code: "VRT_CONFIG_SANDBOX_EVAL_FAILED", message: String(error) }),
    { status: 500, headers: { "content-type": "application/json" } },
  );
}

export default class extends WorkerEntrypoint {
  async fetch(req) {
    let ctx;
    try {
      ctx = await req.json();
    } catch {
      ctx = {};
    }
    let resolved;
    try {
      resolved = await resolveConfig(configExport, ctx);
    } catch (error) {
      return jsonError(error);
    }
    let body;
    try {
      body = JSON.stringify({ ok: true, config: resolved.value, issues: resolved.issues });
    } catch (error) {
      return jsonError(error);
    }
    // Cap the payload so a hostile config cannot make the trusted side
    // buffer without bound.
    if (new TextEncoder().encode(body).length > ${MAX_PAYLOAD_BYTES}) {
      return new Response(
        JSON.stringify({
          ok: false,
          code: "VRT_CONFIG_SANDBOX_PAYLOAD_TOO_LARGE",
          message: "config payload exceeds ${MAX_PAYLOAD_BYTES} bytes",
        }),
        { status: 413, headers: { "content-type": "application/json" } },
      );
    }
    return new Response(body, { status: 200, headers: { "content-type": "application/json" } });
  }
}
`;

function sandboxEnvSource(ctx) {
  return `// Injected before the config module evaluates: workerd has no process, so
// the Node path's allowlisted environment is provided explicitly and frozen.
const env = Object.freeze(${JSON.stringify(ctx.env)});
globalThis.process = Object.freeze({
  env,
  cwd: () => ${JSON.stringify(ctx.cwd)},
  platform: ${JSON.stringify(ctx.platform)},
});
`;
}

function bootstrapSource({ source, chunks, envSource, cpuMs, nonce }) {
  const modules = {
    "eval-config.js": WRAPPER,
    "sandbox-env.js": envSource,
    "cloudflare.config.js": source,
  };
  for (const [name, code] of chunks) {
    if (typeof name === "string" && typeof code === "string" && modules[name] === undefined) {
      modules[name] = code;
    }
  }
  return `const MODULES = ${JSON.stringify(modules)};

export default {
  async fetch(req, env, ctx) {
    // Readiness probe: answers with this run's nonce, without touching the
    // dynamic worker. If the ephemeral port were stolen between freePort()
    // and bind, the probe can never match and startup fails closed instead
    // of POSTing this run's config into a sibling sandbox.
    if (req.method === "GET") return new Response(${JSON.stringify(nonce)});
    const code = {
      compatibilityDate: ${JSON.stringify(COMPAT_DATE)},
      mainModule: "eval-config.js",
      modules: MODULES,
      // Backstop only: upstream workerd ignores this, and the pinned fork
      // terminates the isolate without completing the RPC. The caller's
      // wall-clock deadline is the actual bound.
      limits: { cpuMs: ${Math.max(100, Math.min(cpuMs, 60_000))} },
    };
    const stub = env.LOADER.get("cf-config", async () => code);
    let inner;
    try {
      inner = await stub.getEntrypoint().fetch(
        new Request("http://config.invalid/", { method: "POST", body: await req.text() }),
      );
    } catch (e) {
      return new Response(
        JSON.stringify({ ok: false, code: "VRT_CONFIG_SANDBOX_EVAL_FAILED", message: String(e) }),
        { status: 500, headers: { "content-type": "application/json" } },
      );
    }
    return new Response(await inner.text(), {
      status: inner.status,
      headers: { "content-type": "application/json" },
    });
  },
};
`;
}

function capnp({ port }) {
  return `using Workerd = import "/workerd/workerd.capnp";
const cfg :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [ (name = "bootstrap.js", esModule = embed "bootstrap.js") ],
      compatibilityDate = ${JSON.stringify(COMPAT_DATE)},
      bindings = [ (name = "LOADER", workerLoader = ()) ],
    )),
  ],
  sockets = [ (name = "http", address = "127.0.0.1:${port}", http = (), service = "main") ],
);
`;
}

async function evalInWorkerd({ workDir, workerd, ctx, timeoutMs, nonce }) {
  const port = await freePort();
  writeFileSync(join(workDir, "cfg.capnp"), capnp({ port }));

  const child = spawn(workerd, ["serve", "cfg.capnp", "--experimental"], {
    cwd: workDir,
    stdio: ["ignore", "pipe", "pipe"],
    // Own process group so a timeout kills workerd (and anything it
    // spawned) without touching this process.
    detached: true,
  });
  const logs = [];
  let logBytes = 0;
  const collect = (d) => {
    if (logBytes >= MAX_LOG_BYTES) return;
    logs.push(d.subarray(0, MAX_LOG_BYTES - logBytes));
    logBytes += d.length;
  };
  child.stdout.on("data", collect);
  child.stderr.on("data", collect);
  const exited = new Promise((ok) => child.on("exit", (code, signal) => ok({ code, signal })));
  const spawnError = new Promise((ok) => child.on("error", (e) => ok({ error: String(e) })));

  let stopped = false;
  const stop = async () => {
    if (stopped) return;
    stopped = true;
    if (child.exitCode === null && child.signalCode === null) killTree(child);
    await Promise.race([exited, new Promise((r) => setTimeout(r, 2_000))]);
  };
  // The driver may be killed by its own outer deadline; make a best effort
  // not to leave workerd behind on a normal exit path.
  process.once("exit", () => {
    if (child.exitCode === null && child.signalCode === null) killTree(child);
  });

  try {
    const failed = await Promise.race([
      spawnError,
      waitReady(port, Date.now() + STARTUP_TIMEOUT_MS, nonce).then((ok) =>
        ok ? null : { ready: false },
      ),
    ]);
    if (failed?.error) return { ok: false, phase: "startup", error: `cannot start workerd: ${failed.error}` };
    if (failed?.ready === false) {
      return {
        ok: false,
        phase: "startup",
        error: `workerd did not listen within ${STARTUP_TIMEOUT_MS} ms`,
        logs: Buffer.concat(logs).toString("utf8"),
      };
    }

    const started = process.hrtime.bigint();
    let res;
    let body;
    try {
      res = await fetch(`http://127.0.0.1:${port}/`, {
        method: "POST",
        body: JSON.stringify(ctx),
        signal: AbortSignal.timeout(timeoutMs),
      });
      body = await res.text();
    } catch (e) {
      const timedOut = e?.name === "TimeoutError" || e?.name === "AbortError";
      const logText = Buffer.concat(logs).toString("utf8");
      if (!timedOut) {
        // A crashed isolate or process surfaces as a bare transport error
        // (`fetch failed`). Wait briefly for the exit event so the
        // diagnostic can name the real cause — an allocation abort or an
        // OOM kill — instead of only the failed socket.
        await Promise.race([exited, new Promise((r) => setTimeout(r, 250))]);
      }
      const exitedDetail =
        child.exitCode !== null || child.signalCode !== null
          ? `workerd exited (code ${child.exitCode}, signal ${child.signalCode ?? "none"})`
          : "workerd still running";
      const tail = lastLogLine(logText);
      // Kernel OOM kills and V8 allocation aborts both land here; the code
      // stays LOADER_FAILED, the message says which one it was.
      const outOfMemory = /out of memory|allocation failed|cannot allocate|\bOOM\b|oom[-_]kill/i.test(
        logText,
      );
      const detail = `${truncate(String(e), 200)}; ${exitedDetail}${tail ? `; workerd: ${truncate(tail, 300)}` : ""}`;
      return {
        ok: false,
        phase: "eval",
        timedOut,
        evalMs: ms(started),
        outOfMemory,
        error: timedOut
          ? `config evaluation exceeded ${timeoutMs} ms; workerd sandbox killed`
          : outOfMemory
            ? `config evaluation aborted on a failed memory allocation; ${detail}`
            : `config evaluation failed: ${detail}`,
        logs: logText,
      };
    }
    return {
      ok: true,
      evalMs: ms(started),
      status: res.status,
      body,
      logs: Buffer.concat(logs).toString("utf8"),
    };
  } finally {
    await stop();
  }
}

// ── step 3: trusted validation + conversion ────────────────────────────

function projectIssues(issues) {
  const projected = [];
  for (const issue of issues.slice(0, MAX_ISSUES)) {
    const path = Array.isArray(issue?.path) ? issue.path.join(".") : "";
    const message = truncate(issue?.message ?? issue?.code ?? "invalid");
    const entry = { code: String(issue?.code ?? ""), message };
    if (path) entry.path = path;
    projected.push(entry);
  }
  if (issues.length > MAX_ISSUES) {
    projected.push({ code: "too_many_issues", message: `${issues.length - MAX_ISSUES} more not shown` });
  }
  return projected;
}

async function trustedConvert(config, workerIssues) {
  let api;
  try {
    api = await import("@cloudflare/config");
  } catch (e) {
    return {
      ok: false,
      code: "VRT_CONFIG_LOADER_FAILED",
      message: `@cloudflare/config is not resolvable next to the sandbox driver (${e?.code ?? e?.message ?? "import failed"})`,
    };
  }
  const issues = Array.isArray(workerIssues) ? [...workerIssues] : [];
  if (issues.length > 0) {
    const projected = projectIssues(issues);
    const first = projected[0];
    return {
      ok: false,
      code: "VRT_CONFIG_INVALID",
      issues: projected,
      message: first?.path ? `${first.path}: ${first.message}` : (first?.message ?? "config definition is not valid"),
    };
  }
  const parsed = api.CloudflareConfigSchema.safeParse(config);
  if (!parsed.success) {
    const projected = projectIssues(parsed.error.issues);
    const first = projected[0];
    return {
      ok: false,
      code: "VRT_CONFIG_INVALID",
      issues: projected,
      message: first?.path ? `${first.path}: ${first.message}` : (first?.message ?? "config did not parse"),
    };
  }
  try {
    return { ok: true, raw: api.convertToWranglerConfig(parsed.data) };
  } catch (e) {
    return { ok: false, code: "VRT_CONFIG_LOADER_FAILED", message: `convertToWranglerConfig failed: ${e?.message ?? e}` };
  }
}

// ── main ───────────────────────────────────────────────────────────────

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const requested = resolve(args.configPath);
  if (!existsSync(requested)) usage(`cannot read config file: ${args.configPath}`);
  const configPath = realpathSync(requested);
  // rolldown reports real paths; anchor the project root the same way so the
  // dependency filter and `process.cwd()` are stable under symlinked dirs
  // (`/tmp` → `/private/tmp`).
  const projectDir = realpathSync(dirname(configPath));
  const configFile = relative(projectDir, configPath);

  if (!existsSync(configPath)) usage(`cannot read config file: ${configFile}`);
  const extension = configPath.slice(configPath.lastIndexOf(".")).toLowerCase();
  if (!CONFIG_EXTENSIONS.has(extension)) usage(`unsupported config extension ${extension || "<none>"}`);
  if (statSync(configPath).size > MAX_CONFIG_BYTES) usage(`config file exceeds ${MAX_CONFIG_BYTES} bytes`);

  if (!args.bundler) {
    emitError("VRT_CONFIG_LOADER_FAILED", "of-bundler is not available (set OF_BUNDLER)", { phase: "bundle" });
  }
  if (!existsSync(args.bundler)) {
    emitError("VRT_CONFIG_LOADER_FAILED", "of-bundler is not available (set OF_BUNDLER)", { phase: "bundle" });
  }
  if (!args.workerd) {
    emitError(
      "VRT_CONFIG_LOADER_FAILED",
      "the config worker sandbox needs a workerd binary — set VRT_WORKERD_BIN (or use --config-sandbox=node)",
      { phase: "startup" },
    );
  }
  if (!existsSync(args.workerd)) {
    emitError("VRT_CONFIG_LOADER_FAILED", "VRT_WORKERD_BIN does not point at a file", { phase: "startup" });
  }

  const keep = process.env.OF_CF_SANDBOX_KEEP === "1";
  const workDir = mkdtempSync(join(tmpdir(), "of-cfg-sandbox-"));
  const cleanup = () => {
    if (!keep) rmSync(workDir, { recursive: true, force: true });
  };
  // `emitError` exits the process directly; the exit handler is the only
  // cleanup path that always runs.
  process.once("exit", cleanup);

  try {
    // Only the Node path's allowlisted environment crosses into the worker.
    const env = {};
    for (const key of ENV_ALLOW) {
      if (process.env[key] !== undefined) env[key] = process.env[key];
    }
    const ctx = {
      mode: args.mode,
      isPreview: args.preview,
      env,
      cwd: projectDir,
      platform: process.platform,
    };

    const bundled = await bundle({ projectDir, configFile, bundler: args.bundler, workDir });
    if (!bundled.ok) {
      emitError("VRT_CONFIG_LOADER_FAILED", bundled.error, {
        phase: bundled.phase,
        ...(bundled.code ? { detail: bundled.code } : {}),
      });
    }

    // Per-run nonce: the readiness probe identifies *this* workerd, so a
    // stolen ephemeral port fails closed instead of crossing sandboxes.
    const nonce = randomUUID();
    writeFileSync(
      join(workDir, "bootstrap.js"),
      bootstrapSource({
        source: bundled.source,
        chunks: bundled.chunks,
        envSource: sandboxEnvSource(ctx),
        cpuMs: args.timeoutMs,
        nonce,
      }),
    );

    const evaluated = await evalInWorkerd({
      workDir,
      workerd: args.workerd,
      ctx: { mode: args.mode, isPreview: args.preview },
      timeoutMs: args.timeoutMs,
      nonce,
    });
    if (!evaluated.ok) {
      const timeoutHint =
        evaluated.phase === "eval" && evaluated.timedOut
          ? ` — set VRT_CF_CONFIG_SANDBOX_TIMEOUT_MS to raise the ${args.timeoutMs} ms deadline`
          : "";
      emitError(
        evaluated.timedOut ? "VRT_CONFIG_LOADER_TIMEOUT" : "VRT_CONFIG_LOADER_FAILED",
        `${evaluated.error}${timeoutHint}`,
        { phase: evaluated.phase },
      );
    }
    if (evaluated.status !== 200) {
      let detail = "";
      try {
        const body = JSON.parse(evaluated.body);
        if (body && typeof body.message === "string") detail = `: ${truncate(body.message, 300)}`;
      } catch {
        // non-JSON body — status alone is the diagnostic
      }
      emitError(
        "VRT_CONFIG_LOADER_FAILED",
        `config evaluation failed (HTTP ${evaluated.status})${detail}`,
        { phase: "eval" },
      );
    }

    let payload;
    try {
      payload = JSON.parse(evaluated.body);
    } catch {
      emitError("VRT_CONFIG_LOADER_FAILED", "the config worker returned non-JSON", { phase: "eval" });
    }
    if (!payload || payload.ok !== true) {
      const code = typeof payload?.code === "string" ? payload.code : "VRT_CONFIG_SANDBOX_EVAL_FAILED";
      emitError("VRT_CONFIG_LOADER_FAILED", truncate(payload?.message ?? "config evaluation failed"), {
        phase: "eval",
        detail: code,
      });
    }

    const converted = await trustedConvert(payload.config, payload.issues);
    if (!converted.ok) {
      if (converted.code === "VRT_CONFIG_INVALID") {
        emitError("VRT_CONFIG_INVALID", converted.message, { issues: converted.issues });
      }
      emitError("VRT_CONFIG_LOADER_FAILED", converted.message, { phase: "convert" });
    }

    writeOut(`${JSON.stringify({ raw: converted.raw, dependencies: bundled.dependencies })}\n`);
    if (keep) writeErr(`${JSON.stringify({ note: "sandbox work dir kept", workDir })}\n`);
  } finally {
    cleanup();
  }
}

main().catch((error) => {
  emitError("VRT_CONFIG_LOADER_FAILED", `unexpected sandbox failure: ${error?.message ?? error}`, {
    phase: "driver",
  });
});
