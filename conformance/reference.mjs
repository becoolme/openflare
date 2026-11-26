#!/usr/bin/env node
/**
 * conformance/reference.mjs — run the conformance cases against workerd's
 * **bundled modules** instead of the OpenFlare facades.
 *
 * This is the drift detector behind
 * `docs/bindings/workerd-bundled-modules.md`: every case that we reimplement
 * (`kv`, `d1`, `r2`, `ratelimits`) can also be served by workerd's own
 * `/workerd/<name>.js`, with a local disk. Running the same steps against both
 * stacks turns a semantic difference into a diff instead of a bug report.
 *
 * Nothing here deploys OpenFlare. The reference stack is a single `workerd`
 * process per case:
 *
 *   node conformance/reference.mjs --workerd <path> [--capnp <workerd.capnp>] \
 *     [--only <regex>] [--against conformance/results/latest.json]
 *
 * The config is rendered in the same shape workerd's own `wrangler` CLI uses
 * (`src/workerd/server/cli/wrangler.rs` in the workerd tree), so it exercises
 * the real bundled wiring, not a hand-written approximation.
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { loadCases, runCase, SCHEMA_VERSION } from "./run.mjs";

/** Top-level keys this renderer understands. Anything else makes the case
 * unsupported (we never want to silently drop a declared capability). */
const SUPPORTED_KEYS = new Set([
  "name",
  "main",
  "compatibility_date",
  "compatibility_flags",
  "routes",
  "vars",
  "kv_namespaces",
  "d1_databases",
  "r2_buckets",
  "ratelimits",
]);

const MANIFEST_NAMES = ["wrangler.json", "wrangler.jsonc"];

// ── manifest ────────────────────────────────────────────────────────

/** Strip `//` and `/* *​/` comments plus trailing commas, preserving
 * string contents. wrangler.jsonc is JSONC, not JSON. */
export function stripJsonc(text) {
  let out = "";
  let i = 0;
  let inString = false;
  while (i < text.length) {
    const c = text[i];
    if (inString) {
      out += c;
      if (c === "\\") {
        out += text[i + 1] ?? "";
        i += 2;
        continue;
      }
      if (c === '"') inString = false;
      i++;
      continue;
    }
    if (c === '"') {
      inString = true;
      out += c;
      i++;
      continue;
    }
    if (c === "/" && text[i + 1] === "/") {
      while (i < text.length && text[i] !== "\n") i++;
      continue;
    }
    if (c === "/" && text[i + 1] === "*") {
      const end = text.indexOf("*/", i + 2);
      i = end < 0 ? text.length : end + 2;
      continue;
    }
    out += c;
    i++;
  }
  // Trailing commas: `,` followed only by whitespace and `}`/`]`.
  return out.replace(/,(\s*[}\]])/g, "$1");
}

/** Read the case's wrangler manifest. Returns null when the case uses
 * vrt.toml (no bundled reference mapping — decided in `referenceSupport`). */
export function readReferenceManifest(dir) {
  for (const name of MANIFEST_NAMES) {
    const p = path.join(dir, name);
    if (!fs.existsSync(p)) continue;
    const raw = fs.readFileSync(p, "utf8");
    return { file: name, doc: JSON.parse(stripJsonc(raw)) };
  }
  return null;
}

/** Whether the manifest maps onto bundled modules, and why not. */
export function referenceSupport(manifest) {
  if (!manifest) {
    return { ok: false, reason: "no wrangler manifest (vrt.toml has no bundled reference mapping)" };
  }
  const doc = manifest.doc;
  const unknown = Object.keys(doc).filter((k) => !SUPPORTED_KEYS.has(k));
  if (unknown.length) {
    return {
      ok: false,
      reason: `no bundled reference mapping for: ${unknown.sort().join(", ")}`,
    };
  }
  if (typeof doc.main !== "string" || !doc.main) {
    return { ok: false, reason: "manifest has no main module" };
  }
  if (typeof doc.compatibility_date !== "string" || !doc.compatibility_date) {
    return { ok: false, reason: "manifest has no compatibility_date" };
  }
  return { ok: true };
}

// ── config rendering ────────────────────────────────────────────────

/** Cap'n Proto text literal. JSON string escaping is a valid subset of
 * capnp's; reject control characters we would otherwise smuggle through. */
function q(value) {
  const s = String(value);
  if (/[\u0000-\u001f\u007f]/.test(s)) throw new Error(`control character in ${JSON.stringify(s)}`);
  return JSON.stringify(s);
}

/** A JSON value as a capnp `json = "…"` literal (JSON text inside a string). */
function jsonLiteral(value) {
  return q(JSON.stringify(value));
}

function diskService(name, dir) {
  fs.mkdirSync(dir, { recursive: true });
  return `    (name = ${q(name)}, disk = (path = ${q(dir)}, writable = true)),\n`;
}

/**
 * Render one case as a workerd config that serves it from bundled modules.
 * `port` must be free; the caller binds nothing, workerd does.
 *
 * Returns `{ ok: true, text, files }` where `files` are the worker modules to
 * materialise next to the config, or `{ ok: false, reason }`.
 */
export function renderReferenceConfig({ entry, port, stateDir }) {
  const manifest = readReferenceManifest(entry.dir);
  const support = referenceSupport(manifest);
  if (!support.ok) return support;
  const doc = manifest.doc;
  const name = doc.name || entry.id;

  const modules = fs
    .readdirSync(entry.dir)
    .filter((f) => f.endsWith(".js") || f.endsWith(".mjs") || f.endsWith(".cjs"))
    .sort((a, b) => (a === doc.main ? -1 : b === doc.main ? 1 : a.localeCompare(b)));
  if (!modules.includes(doc.main)) {
    return { ok: false, reason: `main module ${doc.main} not found in ${entry.dir}` };
  }

  const services = [];
  const bindings = [];

  for (const [k, v] of Object.entries(doc.vars ?? {})) {
    bindings.push(
      typeof v === "string"
        ? `        (name = ${q(k)}, text = ${q(v)}),\n`
        : `        (name = ${q(k)}, json = ${jsonLiteral(v)}),\n`,
    );
  }

  for (const kv of doc.kv_namespaces ?? []) {
    const svc = `${name}-kv`;
    const dir = path.join(stateDir, "kv");
    services.push(diskService(`${svc}-storage`, dir));
    services.push(diskService(`${svc}-blobs`, path.join(dir, "blobs")));
    services.push(
      `    (name = ${q(svc)}, worker = (\n` +
        `      compatibilityDate = "2026-01-01",\n` +
        `      modules = [(name = "kv.js", bundled = "/workerd/kv.js")],\n` +
        `      bindings = [\n` +
        `        (name = "SHARD", durableObjectNamespace = (className = "KVNamespace")),\n` +
        `        (name = "BLOBS", service = ${q(`${svc}-blobs`)}),\n` +
        `      ],\n` +
        `      durableObjectNamespaces = [(className = "KVNamespace", uniqueKey = "KVNamespace", enableSql = true)],\n` +
        `      durableObjectStorage = (localDisk = ${q(`${svc}-storage`)}),\n` +
        `    )),\n`,
    );
    bindings.push(
      `        (name = ${q(kv.binding)}, kvNamespace = (name = ${q(svc)}, props = (json = ${jsonLiteral({
        namespace: kv.id ?? kv.binding,
      })}))),\n`,
    );
  }

  for (const db of doc.d1_databases ?? []) {
    const svc = `${name}-d1`;
    const dir = path.join(stateDir, "d1");
    services.push(diskService(`${svc}-storage`, dir));
    services.push(
      `    (name = ${q(svc)}, worker = (\n` +
        `      compatibilityDate = "2026-01-01",\n` +
        `      modules = [(name = "d1.js", bundled = "/workerd/d1.js")],\n` +
        `      bindings = [(name = "DATABASE", durableObjectNamespace = (className = "D1Database"))],\n` +
        `      durableObjectNamespaces = [(className = "D1Database", uniqueKey = "D1Database", enableSql = true)],\n` +
        `      durableObjectStorage = (localDisk = ${q(`${svc}-storage`)}),\n` +
        `    )),\n`,
    );
    bindings.push(
      `        (name = ${q(db.binding)}, wrapped = (moduleName = "cloudflare-internal:d1-api", innerBindings = [\n` +
        `            (name = "fetcher", service = (name = ${q(svc)}, props = (json = ${jsonLiteral({
          database: db.preview_database_id ?? db.database_id ?? db.binding,
        })})))\n` +
        `        ])),\n`,
    );
  }

  for (const bucket of doc.r2_buckets ?? []) {
    const svc = `${name}-r2`;
    const dir = path.join(stateDir, "r2");
    services.push(diskService(`${svc}-storage`, dir));
    services.push(
      `    (name = ${q(`${svc}-blobs`)}, bucket = (path = "r2", localDisk = ${q(`${svc}-storage`)})),\n`,
    );
    services.push(
      `    (name = ${q(svc)}, worker = (\n` +
        `      compatibilityDate = "2026-01-01",\n` +
        `      modules = [(name = "r2.js", bundled = "/workerd/r2.js")],\n` +
        `      bindings = [(name = "BLOBS", service = ${q(`${svc}-blobs`)})],\n` +
        `    )),\n`,
    );
    bindings.push(
      `        (name = ${q(bucket.binding)}, r2Bucket = (name = ${q(svc)}, props = (json = ${jsonLiteral({
        bucket: bucket.bucket_name ?? bucket.binding,
      })}))),\n`,
    );
  }

  const limits = doc.ratelimits ?? [];
  if (limits.length) {
    const svc = `${name}-ratelimit`;
    services.push(
      `    (name = ${q(svc)}, worker = (\n` +
        `      compatibilityDate = "2026-01-01",\n` +
        `      modules = [(name = "ratelimit.js", bundled = "/workerd/ratelimit.js")],\n` +
        `      bindings = [],\n` +
        `    )),\n`,
    );
    for (const r of limits) {
      bindings.push(
        `        (name = ${q(r.name)}, service = (name = ${q(svc)}, entrypoint = "RateLimit", props = (json = ${jsonLiteral({
          namespace: String(r.namespace_id),
          limit: r.simple?.limit,
          period: r.simple?.period,
        })}))),\n`,
      );
    }
  }

  const flags = doc.compatibility_flags ?? [];
  const text =
    `using Workerd = import "/workerd/workerd.capnp";\n` +
    `const config :Workerd.Config = (\n` +
    `  services = [\n` +
    `    (name = "internet", network = (allow = ["public"])),\n` +
    `    (name = ${q(name)}, worker = (\n` +
    `      compatibilityDate = ${q(doc.compatibility_date)},\n` +
    `      modules = [\n` +
    modules.map((m) => `        (name = ${q(m)}, esModule = embed ${q(m)}),\n`).join("") +
    `      ],\n` +
    (flags.length
      ? `      compatibilityFlags = [${flags.map((f) => q(f)).join(", ")}],\n`
      : "") +
    `      bindings = [\n` +
    bindings.join("") +
    `      ],\n` +
    `    )),\n` +
    services.join("") +
    `  ],\n` +
    `  sockets = [(name = "http", address = ${q(`127.0.0.1:${port}`)}, service = ${q(name)})],\n` +
    `);\n`;

  return { ok: true, text, files: modules, name };
}

/** The pinned binary must know `Worker.Module.bundled` (`bundled @11`). */
export function supportsBundledModules(capnpText) {
  return /bundled\s*@\d+/.test(capnpText ?? "");
}

// ── running ─────────────────────────────────────────────────────────

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

function waitForPort(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const attempt = () => {
      const sock = net.connect(port, "127.0.0.1");
      sock.setTimeout(500);
      sock.once("connect", () => {
        sock.destroy();
        resolve();
      });
      const retry = () => {
        sock.destroy();
        if (Date.now() > deadline) reject(new Error(`workerd did not listen on :${port}`));
        else setTimeout(attempt, 100);
      };
      sock.once("error", retry);
      sock.once("timeout", retry);
    };
    attempt();
  });
}

/**
 * Start the reference stack for one case, run its steps, stop it.
 * The result record is the same shape `run.mjs` produces, so reports and
 * comparisons stay uniform.
 */
export async function runReferenceCase(entry, opts) {
  const { workerdBin, timeoutMs = 15_000, readyTimeoutMs = 30_000, log = () => {} } = opts;
  const started = Date.now();
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), `conformance-ref-${entry.id}-`));
  let child;
  try {
    const port = await freePort();
    const rendered = renderReferenceConfig({ entry, port, stateDir: path.join(tmp, "state") });
    if (!rendered.ok) {
      return { id: entry.id, binding: entry.binding, title: entry.case?.title, status: "skipped", reason: rendered.reason, steps: [] };
    }
    for (const m of rendered.files) fs.copyFileSync(path.join(entry.dir, m), path.join(tmp, m));
    const configPath = path.join(tmp, "config.capnp");
    fs.writeFileSync(configPath, rendered.text);

    let stderr = "";
    // workerd gates every bundled module behind --experimental (the
    // fork keeps the gate; vrt-host passes the same flag).
    child = spawn(workerdBin, ["serve", "--experimental", configPath], { cwd: tmp, stdio: ["ignore", "pipe", "pipe"] });
    child.stdout.on("data", (b) => (stderr = (stderr + b.toString()).slice(-8192)));
    child.stderr.on("data", (b) => (stderr = (stderr + b.toString()).slice(-8192)));
    try {
      await waitForPort(port, readyTimeoutMs);
    } catch (e) {
      return {
        id: entry.id,
        binding: entry.binding,
        title: entry.case?.title,
        status: "failed",
        reason: `reference stack did not start: ${e.message}\n${stderr.slice(-2000)}`,
        steps: [],
        durationMs: Date.now() - started,
      };
    }
    log(`  reference: ${rendered.name} on :${port}`);
    const result = await runCase(entry, {
      url: `http://127.0.0.1:${port}`,
      host: `${rendered.name}.localhost`,
      stub: false,
      deployCmd: null,
      deployTimeoutMs: 0,
      readyTimeoutMs,
      timeoutMs,
      keepGoing: opts.keepGoing ?? true,
      log,
    });
    result.durationMs = Date.now() - started;
    return result;
  } finally {
    if (child) {
      child.kill("SIGTERM");
      await new Promise((r) => {
        const t = setTimeout(() => {
          child.kill("SIGKILL");
          r();
        }, 3000);
        child.once("exit", () => {
          clearTimeout(t);
          r();
        });
      });
    }
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

// ── comparison ──────────────────────────────────────────────────────

/**
 * Step-level comparison of a VRT report (`run.mjs` output) and a bundled
 * reference report. `bundledOnly` is the signal that matters: workerd does
 * something our facade does not.
 */
export function compareReports(vrt, bundled) {
  const byId = new Map((bundled?.cases ?? []).map((c) => [c.id, c]));
  const out = { agreements: [], vrtOnly: [], bundledOnly: [], bothFail: [], missing: [] };
  for (const c of vrt?.cases ?? []) {
    const ref = byId.get(c.id);
    if (!ref) {
      out.missing.push({ id: c.id, side: "bundled" });
      continue;
    }
    if (ref.status === "skipped" || c.status === "skipped") continue;
    const refSteps = new Map((ref.steps ?? []).map((s) => [s.name, s.status]));
    for (const s of c.steps ?? []) {
      const status = refSteps.get(s.name);
      if (status === undefined) {
        out.missing.push({ id: c.id, step: s.name, side: "bundled" });
        continue;
      }
      const rec = { id: c.id, step: s.name, vrt: s.status, bundled: status };
      if (s.status === "passed" && status === "passed") out.agreements.push(rec);
      else if (s.status === "passed" && status !== "passed") out.vrtOnly.push(rec);
      else if (s.status !== "passed" && status === "passed") out.bundledOnly.push(rec);
      else out.bothFail.push(rec);
    }
  }
  return out;
}

export function renderComparison(comparison) {
  const lines = ["## Reference comparison", ""];
  lines.push(
    `- agree: **${comparison.agreements.length}** steps`,
    `- only VRT passes: **${comparison.vrtOnly.length}** (we are stricter or differ)`,
    `- only bundled passes: **${comparison.bundledOnly.length}** (workerd does something we do not)`,
    `- both fail: **${comparison.bothFail.length}**`,
    `- unmatched: **${comparison.missing.length}**`,
    "",
  );
  const table = (title, rows, note) => {
    if (!rows.length) return;
    lines.push(`### ${title}`, "", note, "", "| case | step | VRT | bundled |", "| --- | --- | --- | --- |");
    for (const r of rows) lines.push(`| ${r.id} | ${r.step} | ${r.vrt ?? "—"} | ${r.bundled ?? "—"} |`);
    lines.push("");
  };
  table("Only VRT passes", comparison.vrtOnly, "Behaviour we implement that the bundled module does not (or an error-shape difference).");
  table("Only bundled passes", comparison.bundledOnly, "Behaviour workerd implements that we are missing — fix or document.");
  table("Both fail", comparison.bothFail, "The expectation is wrong, or the case needs an OpenFlare-only binding.");
  return lines.join("\n");
}

// ── CLI ─────────────────────────────────────────────────────────────

const USAGE = `usage: node conformance/reference.mjs --workerd <bin> [options]

Run the conformance cases against workerd's bundled modules (kv, d1, r2,
ratelimits) and optionally diff the result against an OpenFlare run.

  --workerd <path>     workerd binary that contains the bundled modules [required]
  --capnp <path>       workerd.capnp of that binary; refuses to run when it has
                       no \`bundled\` module field (older workerd)
  --only <regex>       run only case ids matching the regex
  --against <file>     a run.mjs report (results/latest.json) to compare with
  --results <dir>      output dir (conformance/results)
  --timeout <ms>       per-request timeout (15000)
  --ready-timeout <ms> reference stack start budget (30000)
  --fail-fast          stop a case at its first failed step
`;

function parseArgs(argv) {
  const o = { timeoutMs: 15_000, readyTimeoutMs: 30_000, keepGoing: true };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const val = () => argv[++i];
    switch (a) {
      case "--workerd": o.workerd = val(); break;
      case "--capnp": o.capnp = val(); break;
      case "--only": o.only = new RegExp(val()); break;
      case "--against": o.against = val(); break;
      case "--results": o.resultsDir = val(); break;
      case "--timeout": o.timeoutMs = Number(val()); break;
      case "--ready-timeout": o.readyTimeoutMs = Number(val()); break;
      case "--fail-fast": o.keepGoing = false; break;
      case "-h": case "--help": o.help = true; break;
      default: throw new Error(`unknown flag: ${a}`);
    }
  }
  return o;
}

export async function main(argv, { log = console.log, errlog = console.error } = {}) {
  const here = path.dirname(fileURLToPath(import.meta.url));
  let opts;
  try {
    opts = parseArgs(argv);
  } catch (e) {
    errlog(`${e.message}\n${USAGE}`);
    return 2;
  }
  if (opts.help) {
    log(USAGE);
    return 0;
  }
  if (!opts.workerd) {
    errlog("--workerd is required\n" + USAGE);
    return 2;
  }
  if (!fs.existsSync(opts.workerd)) {
    errlog(`workerd not found: ${opts.workerd}`);
    return 2;
  }
  if (opts.capnp) {
    if (!fs.existsSync(opts.capnp)) {
      errlog(`capnp not found: ${opts.capnp}`);
      return 2;
    }
    if (!supportsBundledModules(fs.readFileSync(opts.capnp, "utf8"))) {
      log(
        "SKIP: this workerd.capnp has no `bundled` module field — the binary predates the bundled modules. " +
          "Use the pinned workerd from deploy/docker/workerd.lock.",
      );
      return 0;
    }
  }

  const entries = loadCases(path.join(here, "cases"));
  const bad = entries.filter((e) => !e.ok);
  for (const e of bad) for (const er of e.errors) errlog(`case ${e.id}: ${er}`);
  if (bad.length) return 2;
  const wanted = opts.only ? entries.filter((e) => opts.only.test(e.id)) : entries;
  if (!wanted.length) {
    errlog("no cases matched --only");
    return 2;
  }

  log(`reference: ${wanted.length} case(s), workerd=${opts.workerd}`);
  const results = [];
  for (const entry of wanted) {
    const r = await runReferenceCase(entry, {
      workerdBin: opts.workerd,
      timeoutMs: opts.timeoutMs,
      readyTimeoutMs: opts.readyTimeoutMs,
      keepGoing: opts.keepGoing,
      log,
    });
    const mark = r.status === "passed" ? "PASS" : r.status === "failed" ? "FAIL" : "SKIP";
    log(`${mark} ${entry.id}${r.reason ? ` (${r.reason.split("\n")[0]})` : ""}`);
    results.push(r);
  }

  const report = {
    schemaVersion: SCHEMA_VERSION,
    generatedAt: new Date().toISOString(),
    stack: "bundled",
    workerd: opts.workerd,
    cases: results,
    totals: {
      passed: results.filter((r) => r.status === "passed").length,
      failed: results.filter((r) => r.status === "failed").length,
      skipped: results.filter((r) => r.status === "skipped").length,
    },
  };
  if (opts.against) {
    const vrt = JSON.parse(fs.readFileSync(opts.against, "utf8"));
    report.comparison = compareReports(vrt, report);
  }

  const dir = opts.resultsDir ?? path.join(here, "results");
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "bundled.json"), JSON.stringify(report, null, 2) + "\n");
  const md = [
    `# Bundled-module reference — ${report.generatedAt}`,
    "",
    `workerd: \`${report.workerd}\` — ${report.totals.passed} passed, ${report.totals.failed} failed, ${report.totals.skipped} skipped`,
    "",
    "| case | binding | status | reason |",
    "| --- | --- | --- | --- |",
    ...results.map((r) => `| ${r.id} | ${r.binding} | ${r.status} | ${r.reason ?? ""} |`),
    "",
  ];
  if (report.comparison) md.push(renderComparison(report.comparison), "");
  fs.writeFileSync(path.join(dir, "bundled.md"), md.join("\n"));

  const t = report.totals;
  log(`\n${t.passed} passed, ${t.failed} failed, ${t.skipped} skipped — ${path.join(dir, "bundled.json")} + ${path.join(dir, "bundled.md")}`);
  if (report.comparison) {
    const c = report.comparison;
    log(
      `comparison: ${c.agreements.length} agree, ${c.vrtOnly.length} VRT-only, ${c.bundledOnly.length} bundled-only, ${c.bothFail.length} both-fail`,
    );
  }
  return t.failed === 0 ? 0 : 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).then(
    (code) => process.exit(code),
    (e) => {
      console.error(e);
      process.exit(2);
    },
  );
}
