#!/usr/bin/env node
// Sandboxed `cloudflare.config.*` evaluation spike.
//
//   node spike/config-sandbox/run.mjs [--case parity|loop|fs-import|no-alias]
//                                     [--workerd PATH] [--bundler PATH]
//                                     [--docker-image IMG] [--json] [--keep]
//
// Chain under test (nothing here is wired into `of deploy`):
//
//   cloudflare.config.ts
//     -> of-bundler (rolldown, platform=neutral) with alias
//        @cloudflare/config -> shims/cloudflare-config-public.mjs
//     -> workerd `workerLoader` binding loads the bundle as a dynamic worker
//     -> bootstrap pulls the config through getEntrypoint().fetch()
//     -> trusted side validates + convertToWranglerConfig() -> RawConfig
//
// Exit 1 means an expectation failed (a step behaved differently from what
// the case asserts), not merely "this step is known to be broken".

import { spawn } from "node:child_process";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, "../..");
const FIXTURE = join(REPO, "vrt-local/tests/fixtures/cf-config");
const NODE_LOADER = join(REPO, "scripts/load-cloudflare-config.mjs");
const COMPAT_DATE = "2026-09-17";
const MAX_PAYLOAD_BYTES = 512 * 1024;

// ── args ────────────────────────────────────────────────────────────────

function parseArgs(argv) {
  const out = { case: "parity", mode: "staging", preview: false, json: false, keep: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--case") out.case = argv[++i];
    else if (a === "--mode") out.mode = argv[++i];
    else if (a === "--preview") out.preview = true;
    else if (a === "--workerd") out.workerd = argv[++i];
    else if (a === "--bundler") out.bundler = argv[++i];
    else if (a === "--docker-image") out.dockerImage = argv[++i];
    else if (a === "--json") out.json = true;
    else if (a === "--keep") out.keep = true;
    else if (a === "--help" || a === "-h") {
      console.log(readFileSync(fileURLToPath(import.meta.url), "utf8").split("\n").slice(1, 18).join("\n"));
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${a}`);
    }
  }
  return out;
}

// ── small helpers ───────────────────────────────────────────────────────

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

async function waitReady(port, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/`, {
        method: "GET",
        signal: AbortSignal.timeout(500),
      });
      if (res.ok) return true;
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  return false;
}

function run(cmd, args, opts = {}) {
  return new Promise((ok) => {
    const child = spawn(cmd, args, { ...opts, stdio: ["pipe", "pipe", "pipe"] });
    const out = [];
    const err = [];
    child.stdout.on("data", (d) => out.push(d));
    child.stderr.on("data", (d) => err.push(d));
    child.on("error", (e) => ok({ code: -1, stdout: "", stderr: String(e) }));
    child.on("close", (code) =>
      ok({ code, stdout: Buffer.concat(out).toString("utf8"), stderr: Buffer.concat(err).toString("utf8") }),
    );
    if (opts.stdin !== undefined) child.stdin.end(opts.stdin);
    else child.stdin.end();
  });
}

function envOr(name, fallback) {
  const v = process.env[name];
  return v === undefined || v === "" ? fallback : v;
}

// ── step 1: bundle ──────────────────────────────────────────────────────

// A `with { type: "cf-worker" }` import is a *specifier*, not a module:
// the Node loader replaces it with a synthetic module whose default export
// is the absolute path. Do the same by aliasing the specifier to a tiny
// generated shim (relative and bare specifiers are both aliased).
function cfWorkerAliases(configSource, projectDir, workDir) {
  const aliases = {};
  const re = /from\s+["']([^"']+)["']\s+with\s*\{\s*type\s*:\s*["']cf-worker["']\s*\}/g;
  let m;
  let n = 0;
  while ((m = re.exec(configSource)) !== null) {
    const spec = m[1];
    const abs = realpathSync(spec.startsWith(".") ? resolve(projectDir, spec) : spec);
    const shim = join(workDir, `cf-worker-${n++}.mjs`);
    writeFileSync(shim, `export default ${JSON.stringify(abs)};\n`);
    aliases[`${spec}$`] = shim;
  }
  return aliases;
}

async function bundle({ projectDir, entry, bundler, workDir, withAlias }) {
  const source = readFileSync(join(projectDir, entry), "utf8");
  const alias = { ...cfWorkerAliases(source, projectDir, workDir) };
  if (withAlias) alias["@cloudflare/config$"] = join(HERE, "shims/cloudflare-config-public.mjs");
  const spec = {
    root: projectDir,
    entry,
    main_dir: "",
    rule_prefix: "",
    rule_globs: [],
    opts: { nodejs_compat: false, alias },
  };
  const started = process.hrtime.bigint();
  const res = await run(bundler, [], { stdin: JSON.stringify(spec) });
  const tookMs = ms(started);
  if (res.code !== 0) {
    return { ok: false, tookMs, error: `of-bundler exited ${res.code}: ${res.stderr.trim()}` };
  }
  let outcome;
  try {
    outcome = JSON.parse(res.stdout);
  } catch {
    return { ok: false, tookMs, error: `of-bundler printed invalid JSON: ${res.stdout.slice(0, 200)}` };
  }
  if (outcome.err) return { ok: false, tookMs, error: outcome.err };
  return {
    ok: true,
    tookMs,
    source: outcome.ok.main_source,
    chunks: outcome.ok.chunks.length,
    externals: outcome.ok.externals.length,
  };
}

// ── step 2: workerd + workerLoader ──────────────────────────────────────

const WRAPPER = `import configExport from "./cloudflare.config.js";
import { WorkerEntrypoint } from "cloudflare:workers";

// Mirrors the loader's normalizeWorkerEntrypoint: a cf-worker import is a
// module namespace whose default export is the specifier string.
function normalizeEntrypoint(config) {
  const worker = config && typeof config === "object" ? config.worker : undefined;
  const ep = worker && typeof worker === "object" ? worker.entrypoint : undefined;
  if (ep && typeof ep === "object" && typeof ep.default === "string") {
    return { ...config, worker: { ...worker, entrypoint: ep.default } };
  }
  return config;
}

export default class extends WorkerEntrypoint {
  async fetch(req) {
    const ctx = await req.json();
    const raw = typeof configExport === "function" ? await configExport(ctx) : configExport;
    // Untrusted JSON boundary: functions drop out, cycles are impossible
    // in parsed config values. Cap the payload so a hostile config cannot
    // make the trusted side buffer without bound.
    let body;
    try {
      body = JSON.stringify({ ok: true, config: normalizeEntrypoint(raw) });
    } catch (e) {
      return new Response(JSON.stringify({ ok: false, error: String(e) }), {
        status: 500,
        headers: { "content-type": "application/json" },
      });
    }
    if (body.length > ${MAX_PAYLOAD_BYTES}) {
      return new Response(JSON.stringify({ ok: false, error: "config payload too large" }), {
        status: 413,
        headers: { "content-type": "application/json" },
      });
    }
    return new Response(body, { status: 200, headers: { "content-type": "application/json" } });
  }
}
`;

function bootstrapSource(configSource) {
  return `const CONFIG_SOURCE = ${JSON.stringify(configSource)};

export default {
  async fetch(req, env) {
    // Readiness probe: answers without touching the dynamic worker so the
    // caller can wait for workerd without burning the eval case.
    if (req.method === "GET") return new Response("ready");
    const code = {
      compatibilityDate: ${JSON.stringify(COMPAT_DATE)},
      mainModule: "eval-config.js",
      modules: {
        "eval-config.js": ${JSON.stringify(WRAPPER)},
        "cloudflare.config.js": CONFIG_SOURCE,
      },
      // Per-invocation ceiling for the dynamic worker. Enforcement is
      // documented in docs/config-sandbox-spike.md.
      limits: { cpuMs: 250 },
    };
    const stub = env.LOADER.get("cf-config", async () => code);
    const ctx = await req.json().catch(() => ({}));
    let inner;
    try {
      inner = await stub.getEntrypoint().fetch(
        new Request("http://config.invalid/", { method: "POST", body: JSON.stringify(ctx) }),
      );
    } catch (e) {
      return new Response(JSON.stringify({ ok: false, error: String(e) }), {
        status: 500,
        headers: { "content-type": "application/json" },
      });
    }
    return new Response(await inner.text(), {
      status: inner.status,
      headers: { "content-type": "application/json" },
    });
  },
};
`;
}

function capnp({ port, host }) {
  return `using Workerd = import "/workerd/workerd.capnp";
const cfg :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [ (name = "bootstrap.js", esModule = embed "bootstrap.js") ],
      compatibilityDate = ${JSON.stringify(COMPAT_DATE)},
      bindings = [ (name = "LOADER", workerLoader = ()) ],
    )),
  ],
  sockets = [ (name = "http", address = "${host}:${port}", http = (), service = "main") ],
);
`;
}

async function evalInWorkerd({ workDir, workerd, dockerImage, ctx, timeoutMs }) {
  const docker = Boolean(dockerImage);
  const port = await freePort();
  const host = docker ? "0.0.0.0" : "127.0.0.1";
  const name = `of-cfg-${port}`;
  writeFileSync(join(workDir, "cfg.capnp"), capnp({ port, host }));

  let cmd;
  let args;
  if (docker) {
    cmd = "docker";
    args = ["run", "--rm", "--name", name, "-p", `${port}:${port}`, "-v", `${workDir}:/w`, "-w", "/w"];
    if (existsSync("/etc/ssl/cert.pem")) {
      args.push("-v", "/etc/ssl/cert.pem:/etc/ssl/certs/ca-certificates.crt:ro");
    }
    args.push("--entrypoint", "/w/workerd", dockerImage, "serve", "cfg.capnp", "--experimental");
  } else {
    cmd = workerd;
    args = ["serve", "cfg.capnp", "--experimental"];
  }

  const started = process.hrtime.bigint();
  const child = spawn(cmd, args, { cwd: workDir, stdio: ["ignore", "pipe", "pipe"] });
  const logs = [];
  child.stdout.on("data", (d) => logs.push(d));
  child.stderr.on("data", (d) => logs.push(d));
  const exited = new Promise((ok) => child.on("exit", (code, signal) => ok({ code, signal })));

  const stop = async () => {
    if (docker) await run("docker", ["rm", "-f", name]);
    child.kill("SIGKILL");
    await exited;
    if (docker) await run("docker", ["rm", "-f", name]);
  };

  try {
    if (!(await waitReady(port))) {
      return { ok: false, phase: "startup", error: "workerd never listened", logs: Buffer.concat(logs).toString() };
    }
    const readyMs = ms(started);
    const evalStarted = process.hrtime.bigint();
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
      return {
        ok: false,
        phase: "eval",
        readyMs,
        evalMs: ms(evalStarted),
        timedOut: e?.name === "TimeoutError",
        error: String(e),
        logs: Buffer.concat(logs).toString(),
      };
    }
    return {
      ok: true,
      readyMs,
      evalMs: ms(evalStarted),
      status: res.status,
      body,
      logs: Buffer.concat(logs).toString(),
    };
  } finally {
    await stop();
  }
}

// ── step 3: trusted side ────────────────────────────────────────────────

async function trustedConvert(config) {
  const api = await import("@cloudflare/config");
  const started = process.hrtime.bigint();
  // The worker only evaluated the config; validation stays on the trusted
  // side, exactly where the Node loader does it.
  const parsed = api.CloudflareConfigSchema.safeParse(config);
  if (!parsed.success) {
    return {
      ok: false,
      tookMs: ms(started),
      error: parsed.error.issues.map((i) => `${(i.path || []).join(".")}: ${i.message}`).join("; "),
    };
  }
  return { ok: true, tookMs: ms(started), raw: api.convertToWranglerConfig(parsed.data) };
}

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((k) => [k, canonical(value[k])]));
  }
  return value;
}

function diff(a, b, path = "") {
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) {
    return a === b ? [] : [`${path}: ${JSON.stringify(a)} != ${JSON.stringify(b)}`];
  }
  if (Array.isArray(a) !== Array.isArray(b)) return [`${path}: array mismatch`];
  if (Array.isArray(a)) {
    if (a.length !== b.length) return [`${path}: length ${a.length} != ${b.length}`];
    return a.flatMap((v, i) => diff(v, b[i], `${path}[${i}]`));
  }
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  return [...keys].flatMap((k) => diff(a[k], b[k], path ? `${path}.${k}` : k));
}

async function nodeBaseline({ projectDir, entry, mode, preview }) {
  const args = [NODE_LOADER, join(projectDir, entry)];
  if (mode !== undefined) args.push("--mode", mode);
  if (preview) args.push("--preview");
  const started = process.hrtime.bigint();
  const res = await run(process.execPath, args, { cwd: REPO });
  const tookMs = ms(started);
  if (res.code !== 0) return { ok: false, tookMs, error: res.stderr.trim() };
  return { ok: true, tookMs, ...JSON.parse(res.stdout) };
}

// ── cases ───────────────────────────────────────────────────────────────

function materialize(projectDir, files) {
  for (const [name, body] of Object.entries(files)) {
    const p = join(projectDir, name);
    mkdirSync(dirname(p), { recursive: true });
    writeFileSync(p, body);
  }
}

function caseProject(kase) {
  if (kase === "parity") {
    return { projectDir: FIXTURE, entry: "cloudflare.config.ts" };
  }
  const dir = mkdtempSync(join(tmpdir(), `of-cfg-${kase}-`));
  if (kase === "loop") {
    materialize(dir, {
      "cloudflare.config.ts": `import { defineConfig } from "@cloudflare/config";
export default defineConfig(() => {
  while (true) {}
});
`,
    });
  } else if (kase === "fs-import") {
    materialize(dir, {
      "cloudflare.config.ts": `import { readFileSync } from "node:fs";
import { defineConfig } from "@cloudflare/config";
export default defineConfig(() => ({
  worker: {
    name: "fs",
    compatibilityDate: "${COMPAT_DATE}",
    env: { LEAK: readFileSync("/etc/passwd", "utf8") },
  },
}));
`,
    });
  } else if (kase === "network") {
    materialize(dir, {
      "cloudflare.config.ts": `import { defineConfig } from "@cloudflare/config";
export default defineConfig(async () => {
  const res = await fetch("http://169.254.169.254/latest/meta-data/");
  return {
    worker: {
      name: "net",
      compatibilityDate: "${COMPAT_DATE}",
      env: { BODY: await res.text() },
    },
  };
});
`,
    });
  } else {
    throw new Error(`unknown case: ${kase}`);
  }
  return { projectDir: dir, entry: "cloudflare.config.ts", cleanup: dir };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const bundler = args.bundler ?? envOr("OF_BUNDLER", join(REPO, "of-bundler/target/debug/of-bundler"));
  const workerdRaw = args.workerd ?? envOr("OF_WORKERD_BIN", envOr("VRT_WORKERD_BIN", "workerd"));
  // Spawned with cwd = the generated work dir, so a relative path must be
  // anchored now. A bare command name stays a PATH lookup.
  const workerd = workerdRaw.includes("/") ? resolve(workerdRaw) : workerdRaw;
  const report = { case: args.case, startedAt: new Date().toISOString(), steps: {} };
  if (args.dockerImage) report.dockerImage = args.dockerImage;
  else {
    const v = await run(workerd, ["--version"]);
    if (v.code === 0) report.workerdVersion = v.stdout.trim();
  }

  if (!existsSync(bundler)) {
    throw new Error(`of-bundler not found: ${bundler} (run: cargo build --manifest-path of-bundler/Cargo.toml)`);
  }
  if (args.dockerImage && !existsSync(workerd)) {
    throw new Error("--docker-image needs --workerd <linux workerd binary>");
  }
  if (!args.dockerImage && workerd !== "workerd" && !existsSync(workerd)) {
    throw new Error(`workerd not found: ${workerd} (pass --workerd or --docker-image)`);
  }

  const { projectDir, entry, cleanup } = caseProject(args.case);
  const workDir = mkdtempSync(join(tmpdir(), "of-cfg-work-"));
  const cleanups = [workDir, ...(cleanup ? [cleanup] : [])];
  const cleanupAll = () => {
    if (args.keep) return;
    for (const d of cleanups) rmSync(d, { recursive: true, force: true });
  };

  try {
    if (args.dockerImage) {
      // The linux workerd must be visible inside the mounted work dir.
      writeFileSync(join(workDir, "workerd"), readFileSync(workerd));
      chmodSync(join(workDir, "workerd"), 0o755);
      const v = await run("docker", [
        "run", "--rm", "--entrypoint", "/w/workerd", "-v", `${workDir}:/w`, args.dockerImage, "--version",
      ]);
      if (v.code === 0) report.workerdVersion = v.stdout.trim();
    }

    const bundled = await bundle({
      projectDir,
      entry,
      bundler,
      workDir,
      withAlias: true,
    });
    report.steps.bundle = {
      ok: bundled.ok,
      tookMs: bundled.tookMs,
      bytes: bundled.source?.length,
      chunks: bundled.chunks,
      externals: bundled.externals,
      error: bundled.error,
    };

    // Rejection cases: the chain must refuse the config *somewhere*
    // (bundle or dynamic-worker load). A timeout is not a rejection.
    if (args.case === "fs-import" || args.case === "network") {
      if (!bundled.ok) {
        report.verdict = `blocked at bundle: ${bundled.error}`;
        return finish(report, args, null, cleanupAll);
      }
      writeFileSync(join(workDir, "bootstrap.js"), bootstrapSource(bundled.source));
      const evaluated = await evalInWorkerd({
        workDir,
        workerd: args.dockerImage ? "/w/workerd" : workerd,
        dockerImage: args.dockerImage,
        ctx: { mode: args.mode, isPreview: args.preview },
        timeoutMs: Number(envOr("OF_CF_SANDBOX_TIMEOUT_MS", "15000")),
      });
      report.steps.workerd = { ...evaluated, logs: undefined };
      report.workerdLogs = evaluated.logs?.trim() || "";
      if (evaluated.timedOut) return finish(report, args, "unexpected timeout", cleanupAll);
      if (!evaluated.ok || evaluated.status >= 400) {
        report.verdict = `blocked at eval: HTTP ${evaluated.status ?? "-"} ${(evaluated.body || evaluated.error || "").slice(0, 200)}`;
        return finish(report, args, null, cleanupAll);
      }
      return finish(report, args, "the sandbox let a host/network access through", cleanupAll);
    }

    if (!bundled.ok) return finish(report, args, `bundle failed: ${bundled.error}`, cleanupAll);

    writeFileSync(join(workDir, "bootstrap.js"), bootstrapSource(bundled.source));
    const evaluated = await evalInWorkerd({
      workDir,
      workerd: args.dockerImage ? "/w/workerd" : workerd,
      dockerImage: args.dockerImage,
      ctx: { mode: args.mode, isPreview: args.preview },
      timeoutMs: Number(envOr("OF_CF_SANDBOX_TIMEOUT_MS", "15000")),
    });
    report.steps.workerd = { ...evaluated, logs: undefined };
    report.workerdLogs = evaluated.logs?.trim() || "";

    if (args.case === "loop") {
      if (evaluated.ok) return finish(report, args, "a `while (true)` config returned a result", cleanupAll);
      if (!evaluated.timedOut) {
        return finish(report, args, `expected a wall-clock timeout, got: ${evaluated.error}`, cleanupAll);
      }
      // Today: the isolate hits its CPU budget, but the RPC caller is not
      // notified, so only the caller's wall-clock deadline bounds it.
      report.verdict = "timeout (CPU budget hit; RPC caller never observes the failure)";
      return finish(report, args, null, cleanupAll);
    }

    if (!evaluated.ok) return finish(report, args, `workerd eval failed: ${evaluated.error}`, cleanupAll);
    if (evaluated.status !== 200) {
      return finish(report, args, `workerd returned ${evaluated.status}: ${evaluated.body.slice(0, 300)}`, cleanupAll);
    }

    let payload;
    try {
      payload = JSON.parse(evaluated.body);
    } catch {
      return finish(report, args, "workerd returned non-JSON", cleanupAll);
    }
    if (!payload.ok) return finish(report, args, `config evaluation failed: ${payload.error}`, cleanupAll);

    const converted = await trustedConvert(payload.config);
    report.steps.trusted = { ok: converted.ok, tookMs: converted.tookMs, error: converted.error, raw: converted.raw };
    if (!converted.ok) return finish(report, args, `trusted validation failed: ${converted.error}`, cleanupAll);

    const baseline = await nodeBaseline({ projectDir, entry, mode: args.mode, preview: args.preview });
    report.steps.nodeBaseline = { ok: baseline.ok, tookMs: baseline.tookMs, error: baseline.error, raw: baseline.raw };
    if (!baseline.ok) return finish(report, args, `node baseline failed: ${baseline.error}`, cleanupAll);

    const d = diff(canonical(converted.raw), canonical(baseline.raw));
    report.steps.parity = { equal: d.length === 0, diff: d };
    if (d.length > 0) {
      return finish(report, args, `raw config differs from the Node path: ${d.join("; ")}`, cleanupAll);
    }

    // Informational: does the Node main entry still tree-shake down to the
    // `./public` shim for this config? The alias stays the intended route
    // either way -- this only records how much the alias buys today.
    const noAlias = await bundle({ projectDir, entry, bundler, workDir, withAlias: false });
    report.steps.alias = {
      withoutAliasOk: noAlias.ok,
      withoutAliasBytes: noAlias.source?.length,
      identicalToAlias: noAlias.ok && noAlias.source === bundled.source,
      error: noAlias.error,
    };

    report.verdict = "identical RawConfig";
    return finish(report, args, null, cleanupAll);
  } finally {
    cleanupAll();
  }
}

function finish(report, args, error, cleanupAll) {
  cleanupAll();
  report.finishedAt = new Date().toISOString();
  report.ok = error === null;
  if (error) report.error = error;
  if (args.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    const s = report.steps;
    console.log(`case: ${report.case}`);
    if (s.bundle) {
      const suffix = s.bundle.error ? ` — ${s.bundle.error}` : "";
      console.log(
        `  bundle: ${s.bundle.ok ? "ok" : "FAILED"} ${s.bundle.tookMs}ms${s.bundle.bytes ? ` ${s.bundle.bytes}B` : ""}${suffix}`,
      );
    }
    if (s.workerd) {
      const suffix = s.workerd.error ? ` — ${s.workerd.error}` : "";
      console.log(
        `  workerd: ${s.workerd.ok ? "ok" : "FAILED"} start=${s.workerd.readyMs ?? "-"}ms eval=${s.workerd.evalMs ?? "-"}ms status=${s.workerd.status ?? "-"}${suffix}`,
      );
    }
    if (s.trusted) console.log(`  trusted: ${s.trusted.ok ? "ok" : "FAILED"} ${s.trusted.tookMs}ms`);
    if (s.nodeBaseline) console.log(`  node baseline: ${s.nodeBaseline.ok ? "ok" : "FAILED"} ${s.nodeBaseline.tookMs}ms`);
    if (s.parity) console.log(`  parity: ${s.parity.equal ? "identical" : `DIFF ${s.parity.diff.join("; ")}`}`);
    if (s.alias) {
      console.log(
        `  alias: without-alias=${s.alias.withoutAliasOk ? `${s.alias.withoutAliasBytes}B` : "FAILED"} identical=${s.alias.identicalToAlias}`,
      );
    }
    if (report.verdict) console.log(`  verdict: ${report.verdict}`);
    console.log(report.ok ? "RESULT: PASS" : `RESULT: FAIL — ${error}`);
  }
  process.exitCode = report.ok ? 0 : 1;
}

main().catch((e) => {
  console.error(`fatal: ${e?.stack ?? e}`);
  process.exitCode = 1;
});
