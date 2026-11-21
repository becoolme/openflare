#!/usr/bin/env node
/**
 * conformance/run.mjs — binding-layer conformance runner for OpenFlare.
 *
 * Executes the declarative cases under conformance/cases/<binding>/<id>/
 * against a running stack: optionally deploys each case's worker project
 * through a caller-provided deploy command, then issues the case's HTTP
 * steps at the ingress (--url) with the right Host header, checks each
 * response against the expected "golden" result, and writes a JSON
 * report (results/latest.json) plus a Markdown summary.
 *
 * Nothing here runs workerd itself — the stack under test is external.
 * See conformance/README.md for usage.
 */
import { execFile } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import https from "node:https";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const SCHEMA_VERSION = 1;

/** Bindings the suite knows about — used for --only filtering groups and
 * the real-projects manifest vocabulary. Cases may name other bindings
 * (the suite is open-ended), so this is not a closed enum for cases. */
export const KNOWN_BINDINGS = [
  "kv",
  "r2",
  "d1",
  "queues",
  "cache",
  "cron",
  "durable-objects",
  "workflows",
  "service-bindings",
  "assets",
  "ratelimit",
  "hyperdrive",
  "vars",
  "websockets",
  "ai",
  "email",
  "vectorize",
  "node-compat",
];

const ID_RE = /^[a-z0-9][a-z0-9-]{0,63}$/;
const MANIFEST_FILES = [
  "vrt.toml",
  "wrangler.json",
  "wrangler.jsonc",
  "wrangler.toml",
];
const REQUEST_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];

// ── validation ──────────────────────────────────────────────────────

function isRecord(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function err(errors, path, msg) {
  errors.push(`${path}: ${msg}`);
}

function validateRequest(req, errors, where) {
  if (!isRecord(req)) return err(errors, where, "request must be an object");
  if (req.method !== undefined) {
    if (typeof req.method !== "string" || !REQUEST_METHODS.includes(req.method))
      err(errors, `${where}.method`, `must be one of ${REQUEST_METHODS.join("/")}`);
  }
  if (typeof req.path !== "string" || !req.path.startsWith("/"))
    err(errors, `${where}.path`, "must be a string starting with /");
  if (req.headers !== undefined && !isRecord(req.headers))
    err(errors, `${where}.headers`, "must be an object");
  else if (isRecord(req.headers)) {
    for (const [k, v] of Object.entries(req.headers))
      if (typeof v !== "string") err(errors, `${where}.headers.${k}`, "must be a string");
  }
  const bodies = ["body", "json", "bodyBase64"].filter((k) => req[k] !== undefined);
  if (bodies.length > 1)
    err(errors, where, `only one of body/json/bodyBase64 allowed, got ${bodies.join("+")}`);
  if (req.body !== undefined && typeof req.body !== "string")
    err(errors, `${where}.body`, "must be a string");
  if (req.bodyBase64 !== undefined && !/^[A-Za-z0-9+/=]*$/.test(req.bodyBase64 ?? ""))
    err(errors, `${where}.bodyBase64`, "must be base64");
}

function validateExpect(exp, errors, where) {
  if (!isRecord(exp)) return err(errors, where, "expect must be an object");
  const known = ["status", "headers", "body", "bodyContains", "bodyMatches", "json", "jsonEquals"];
  for (const k of Object.keys(exp))
    if (!known.includes(k)) err(errors, `${where}.${k}`, `unknown expect field (allowed: ${known.join(", ")})`);
  if (exp.status !== undefined) {
    const list = Array.isArray(exp.status) ? exp.status : [exp.status];
    if (list.length === 0 || !list.every((s) => Number.isInteger(s) && s >= 100 && s <= 599))
      err(errors, `${where}.status`, "must be an HTTP status int or non-empty list of ints");
  }
  if (exp.headers !== undefined) {
    if (!isRecord(exp.headers)) err(errors, `${where}.headers`, "must be an object");
    else
      for (const [k, v] of Object.entries(exp.headers)) {
        const okStr = typeof v === "string";
        const okShape =
          isRecord(v) &&
          Object.keys(v).every((m) => ["equals", "contains", "matches", "absent"].includes(m)) &&
          Object.keys(v).length > 0;
        if (!okStr && !okShape)
          err(errors, `${where}.headers.${k}`, "must be a string or {equals|contains|matches|absent}");
      }
  }
  if (exp.body !== undefined && typeof exp.body !== "string")
    err(errors, `${where}.body`, "must be a string");
  if (exp.bodyContains !== undefined && !Array.isArray(exp.bodyContains))
    err(errors, `${where}.bodyContains`, "must be an array of strings");
  else if (Array.isArray(exp.bodyContains))
    exp.bodyContains.forEach((s, i) => {
      if (typeof s !== "string") err(errors, `${where}.bodyContains[${i}]`, "must be a string");
    });
  if (exp.bodyMatches !== undefined) {
    if (typeof exp.bodyMatches !== "string") err(errors, `${where}.bodyMatches`, "must be a regex string");
    else {
      try {
        new RegExp(exp.bodyMatches);
      } catch (e) {
        err(errors, `${where}.bodyMatches`, `bad regex: ${e.message}`);
      }
    }
  }
  if (exp.json !== undefined && !isRecord(exp.json))
    err(errors, `${where}.json`, "must be an object (subset match)");
}

function validateStep(step, errors, where) {
  if (!isRecord(step)) return err(errors, where, "step must be an object");
  for (const k of Object.keys(step))
    if (!["name", "request", "expect", "retry"].includes(k))
      err(errors, `${where}.${k}`, "unknown step field");
  if (typeof step.name !== "string" || !step.name)
    err(errors, `${where}.name`, "required, non-empty string");
  validateRequest(step.request, errors, `${where}.request`);
  validateExpect(step.expect ?? {}, errors, `${where}.expect`);
  if (step.retry !== undefined) {
    if (!isRecord(step.retry)) err(errors, `${where}.retry`, "must be an object");
    else {
      for (const k of Object.keys(step.retry))
        if (!["timeoutMs", "intervalMs"].includes(k))
          err(errors, `${where}.retry.${k}`, "unknown retry field");
      if (!Number.isFinite(step.retry.timeoutMs) || step.retry.timeoutMs <= 0)
        err(errors, `${where}.retry.timeoutMs`, "required, positive number");
      if (step.retry.intervalMs !== undefined && (!Number.isFinite(step.retry.intervalMs) || step.retry.intervalMs <= 0))
        err(errors, `${where}.retry.intervalMs`, "must be a positive number");
    }
  }
}

/** Validate a case.json object. Returns {ok, errors, case}. */
export function validateCase(raw, dirname) {
  const errors = [];
  if (!isRecord(raw)) {
    return { ok: false, errors: ["case.json: must be an object"], case: null };
  }
  const known = [
    "schemaVersion", "id", "binding", "title", "requiresWorkerd", "evidence",
    "manifest", "hostname", "readyTimeoutMs", "notes", "steps", "tags",
  ];
  for (const k of Object.keys(raw))
    if (!known.includes(k)) err(errors, k, `unknown case field (allowed: ${known.join(", ")})`);
  if (raw.schemaVersion !== SCHEMA_VERSION)
    err(errors, "schemaVersion", `must be ${SCHEMA_VERSION}`);
  if (typeof raw.id !== "string" || !ID_RE.test(raw.id))
    err(errors, "id", `required, must match ${ID_RE}`);
  else if (dirname && raw.id !== dirname)
    err(errors, "id", `must equal the case directory name (${dirname})`);
  if (typeof raw.binding !== "string" || !raw.binding)
    err(errors, "binding", "required, non-empty string");
  if (typeof raw.title !== "string" || !raw.title)
    err(errors, "title", "required, non-empty string");
  if (raw.requiresWorkerd !== undefined && typeof raw.requiresWorkerd !== "boolean")
    err(errors, "requiresWorkerd", "must be a boolean");
  if (!Array.isArray(raw.evidence) || raw.evidence.length === 0 ||
      !raw.evidence.every((e) => typeof e === "string" && e.length > 0))
    err(errors, "evidence", "required: non-empty array of doc/test references");
  if (raw.manifest !== undefined && (typeof raw.manifest !== "string" || !MANIFEST_FILES.includes(raw.manifest)))
    err(errors, "manifest", `must name one of ${MANIFEST_FILES.join("/")}`);
  if (raw.hostname !== undefined && (typeof raw.hostname !== "string" || !/^[a-z0-9.-]+$/i.test(raw.hostname)))
    err(errors, "hostname", "must be a hostname string");
  if (raw.readyTimeoutMs !== undefined && (!Number.isFinite(raw.readyTimeoutMs) || raw.readyTimeoutMs <= 0))
    err(errors, "readyTimeoutMs", "must be a positive number");
  if (raw.notes !== undefined && typeof raw.notes !== "string")
    err(errors, "notes", "must be a string");
  if (!Array.isArray(raw.steps) || raw.steps.length === 0)
    err(errors, "steps", "required: non-empty array");
  else raw.steps.forEach((s, i) => validateStep(s, errors, `steps[${i}]`));
  return { ok: errors.length === 0, errors, case: errors.length === 0 ? raw : null };
}

/** Load + validate every case under casesDir (one dir per case). */
export function loadCases(casesDir) {
  const out = [];
  const seen = new Set();
  const dirs = fs
    .readdirSync(casesDir, { withFileTypes: true })
    .filter((e) => e.isDirectory())
    .flatMap((bindingDir) =>
      fs
        .readdirSync(path.join(casesDir, bindingDir.name), { withFileTypes: true })
        .filter((e) => e.isDirectory())
        .map((e) => ({ binding: bindingDir.name, id: e.name, dir: path.join(casesDir, bindingDir.name, e.name) })),
    )
    .sort((a, b) => a.dir.localeCompare(b.dir));
  for (const { binding, id, dir } of dirs) {
    const cj = path.join(dir, "case.json");
    if (!fs.existsSync(cj)) {
      out.push({ id, binding, dir, ok: false, errors: [`${dir}: missing case.json`], case: null });
      continue;
    }
    let raw;
    try {
      raw = JSON.parse(fs.readFileSync(cj, "utf8"));
    } catch (e) {
      out.push({ id, binding, dir, ok: false, errors: [`case.json: ${e.message}`], case: null });
      continue;
    }
    const v = validateCase(raw, id);
    if (raw.binding && raw.binding !== binding)
      v.errors.push(`binding: case dir is "${binding}" but case.json says "${raw.binding}"`), (v.ok = false);
    if (seen.has(id)) v.errors.push(`id: duplicate case id "${id}"`), (v.ok = false);
    seen.add(id);
    // manifest + worker files must exist for the deploy stage.
    const manifest = raw.manifest ?? MANIFEST_FILES.find((m) => fs.existsSync(path.join(dir, m)));
    if (!manifest) v.errors.push(`${dir}: no deploy manifest (${MANIFEST_FILES.join(" | ")})`), (v.ok = false);
    else if (!fs.existsSync(path.join(dir, manifest)))
      v.errors.push(`${dir}: manifest "${manifest}" not found`), (v.ok = false);
    out.push({ id, binding, dir, ok: v.ok, errors: v.errors, case: v.ok ? { ...raw, manifest } : null });
  }
  return out;
}

/** Hostname for a case: explicit field wins; else parse the manifest
 * (vrt.toml `hostnames = [...]` first entry / wrangler `routes` first
 * entry stripped of its `/*` suffix). */
export function resolveHostname(entry) {
  if (entry.case?.hostname) return entry.case.hostname;
  const manifestPath = entry.case?.manifest && path.join(entry.dir, entry.case.manifest);
  if (manifestPath && fs.existsSync(manifestPath)) {
    const text = fs.readFileSync(manifestPath, "utf8");
    const hn = text.match(/hostnames\s*=\s*\[\s*"([^"]+)"/);
    if (hn) return hn[1];
    const route = text.match(/"routes"\s*:\s*\[\s*"([^"/*]+)/);
    if (route) return route[1];
    const tomlRoute = text.match(/routes\s*=\s*\[\s*"([^"/*]+)/);
    if (tomlRoute) return tomlRoute[1];
  }
  return null;
}

// ── expect matching ─────────────────────────────────────────────────

function statusMatches(want, got) {
  return (Array.isArray(want) ? want : [want]).includes(got);
}

/** Recursive subset match: every leaf of `want` must deep-equal the
 * same position in `got`; objects recurse, arrays and primitives must
 * be exactly equal. Returns a list of mismatch paths. */
export function subsetDiffs(want, got, at = "") {
  const diffs = [];
  if (isRecord(want)) {
    if (!isRecord(got)) {
      diffs.push(`${at || "$"}: expected object, got ${JSON.stringify(got)}`);
      return diffs;
    }
    for (const [k, v] of Object.entries(want)) {
      if (!(k in got)) diffs.push(`${at}.${k}: missing (expected ${JSON.stringify(v)})`);
      else diffs.push(...subsetDiffs(v, got[k], `${at}.${k}`));
    }
    return diffs;
  }
  // Arrays subset-match positionally: same length, and every expected
  // element must subset-match the element at the same index. (Extra
  // keys inside got elements are fine — object rules apply per index.)
  if (Array.isArray(want)) {
    if (!Array.isArray(got)) {
      diffs.push(`${at || "$"}: expected array, got ${JSON.stringify(truncateObj(got))}`);
      return diffs;
    }
    if (want.length !== got.length) {
      diffs.push(`${at || "$"}: expected ${want.length} elements, got ${got.length}`);
      return diffs;
    }
    want.forEach((v, i) => diffs.push(...subsetDiffs(v, got[i], `${at}[${i}]`)));
    return diffs;
  }
  if (JSON.stringify(want) !== JSON.stringify(got))
    diffs.push(`${at || "$"}: expected ${JSON.stringify(want)}, got ${JSON.stringify(got)}`);
  return diffs;
}

function headerValue(headers, name) {
  const v = headers[name.toLowerCase()];
  return v === undefined ? null : v;
}

/** Check one step's `expect` against the actual response. Returns the
 * list of human-readable diffs ([] = pass). */
export function matchExpect(expect, actual) {
  const diffs = [];
  const exp = expect ?? {};
  const status = exp.status ?? 200;
  if (!statusMatches(status, actual.status))
    diffs.push(`status: expected ${JSON.stringify(status)}, got ${actual.status}`);
  if (exp.headers) {
    for (const [name, rule] of Object.entries(exp.headers)) {
      const got = headerValue(actual.headers, name);
      const r = isRecord(rule) ? rule : { equals: rule };
      if (r.absent === true) {
        if (got !== null) diffs.push(`headers.${name}: expected absent, got ${JSON.stringify(got)}`);
        continue;
      }
      if (got === null) {
        diffs.push(`headers.${name}: missing`);
        continue;
      }
      if (r.equals !== undefined && got !== r.equals)
        diffs.push(`headers.${name}: expected ${JSON.stringify(r.equals)}, got ${JSON.stringify(got)}`);
      if (r.contains !== undefined && !got.includes(r.contains))
        diffs.push(`headers.${name}: expected to contain ${JSON.stringify(r.contains)}, got ${JSON.stringify(got)}`);
      if (r.matches !== undefined && !new RegExp(r.matches).test(got))
        diffs.push(`headers.${name}: expected match /${r.matches}/, got ${JSON.stringify(got)}`);
    }
  }
  if (exp.body !== undefined && actual.bodyText !== exp.body)
    diffs.push(`body: expected ${JSON.stringify(exp.body)}, got ${JSON.stringify(truncate(actual.bodyText))}`);
  for (const s of exp.bodyContains ?? [])
    if (!actual.bodyText.includes(s))
      diffs.push(`body: missing substring ${JSON.stringify(s)} (body ${JSON.stringify(truncate(actual.bodyText))})`);
  if (exp.bodyMatches !== undefined && !new RegExp(exp.bodyMatches).test(actual.bodyText))
    diffs.push(`body: no match /${exp.bodyMatches}/ (body ${JSON.stringify(truncate(actual.bodyText))})`);
  if (exp.json !== undefined || exp.jsonEquals !== undefined) {
    let parsed;
    try {
      parsed = JSON.parse(actual.bodyText);
    } catch {
      diffs.push(`body: not JSON (${JSON.stringify(truncate(actual.bodyText))})`);
      parsed = undefined;
    }
    if (parsed !== undefined) {
      if (exp.json !== undefined) diffs.push(...subsetDiffs(exp.json, parsed));
      if (exp.jsonEquals !== undefined && JSON.stringify(sortKeys(exp.jsonEquals)) !== JSON.stringify(sortKeys(parsed)))
        diffs.push(`body json: expected exactly ${JSON.stringify(exp.jsonEquals)}, got ${JSON.stringify(truncateObj(parsed))}`);
    }
  }
  return diffs;
}

function truncate(s, n = 400) {
  return s.length > n ? s.slice(0, n) + `…(${s.length}b)` : s;
}
function truncateObj(o) {
  const s = JSON.stringify(o);
  return s.length > 400 ? s.slice(0, 400) + "…" : o;
}
function sortKeys(v) {
  if (Array.isArray(v)) return v.map(sortKeys);
  if (isRecord(v))
    return Object.fromEntries(Object.keys(v).sort().map((k) => [k, sortKeys(v[k])]));
  return v;
}

// ── execution ───────────────────────────────────────────────────────

function buildRequest(step) {
  const req = step.request ?? {};
  const method = req.method ?? "GET";
  const headers = { ...(req.headers ?? {}) };
  let body;
  if (req.json !== undefined) {
    body = JSON.stringify(req.json);
    if (!Object.keys(headers).some((h) => h.toLowerCase() === "content-type"))
      headers["content-type"] = "application/json";
  } else if (req.bodyBase64 !== undefined) {
    body = Buffer.from(req.bodyBase64, "base64");
  } else if (req.body !== undefined) {
    body = req.body;
  }
  return { method, headers, body };
}

// Uses node:http/https rather than fetch(): the `host` header is
// forbidden in the Fetch spec and undici silently drops it, but tenant
// selection at the ingress depends on sending the case's Host verbatim.
function oneRequest(baseUrl, host, step, timeoutMs) {
  const { method, headers, body } = buildRequest(step);
  const url = new URL(step.request.path, baseUrl);
  const mod = url.protocol === "https:" ? https : http;
  return new Promise((resolve, reject) => {
    const req = mod.request(
      url,
      { method, headers: { ...headers, host }, timeout: timeoutMs },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const buf = Buffer.concat(chunks);
          const headersObj = {};
          for (const [k, v] of Object.entries(res.headers)) {
            const key = k.toLowerCase();
            headersObj[key] = headersObj[key] === undefined ? v : `${headersObj[key]}, ${v}`;
          }
          resolve({
            status: res.statusCode,
            headers: headersObj,
            bodyText: buf.toString("utf8"),
            bodyBytes: buf.length,
          });
        });
        res.on("error", reject);
      },
    );
    req.on("timeout", () => req.destroy(new Error(`request timed out after ${timeoutMs}ms`)));
    req.on("error", reject);
    if (body !== undefined) req.write(body);
    req.end();
  });
}

async function runStepWithRetry(baseUrl, host, step, timeoutMs, retry) {
  const deadline = retry ? Date.now() + retry.timeoutMs : 0;
  const interval = retry?.intervalMs ?? 1000;
  let last;
  for (;;) {
    try {
      const actual = await oneRequest(baseUrl, host, step, timeoutMs);
      const diffs = matchExpect(step.expect, actual);
      last = { ok: diffs.length === 0, diffs, actual };
      if (last.ok || !retry || Date.now() > deadline) return last;
    } catch (e) {
      last = { ok: false, diffs: [`request failed: ${e.cause?.code ?? e.message}`], actual: null };
      if (!retry || Date.now() > deadline) return last;
    }
    await new Promise((r) => setTimeout(r, interval));
  }
}

function copyTree(src, dest) {
  for (const f of fs.readdirSync(src)) {
    const from = path.join(src, f);
    const to = path.join(dest, f);
    const st = fs.statSync(from);
    if (st.isDirectory()) {
      fs.mkdirSync(to);
      copyTree(from, to);
    } else if (st.isFile()) {
      fs.copyFileSync(from, to);
    }
  }
}

export function stageCaseDir(entry) {
  // Stage a deployable worker dir: every module and asset subtree, minus
  // the top-level case metadata and docs (case.json would deploy as a
  // stray json module; `vrt deploy` rejects markdown extensions). Nested
  // directories are copied recursively — `assets-basic` deploys its
  // `assets.directory = "public"`, which must reach CASE_DIR.
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), `conformance-${entry.id}-`));
  for (const f of fs.readdirSync(entry.dir)) {
    if (f === "case.json" || f.endsWith(".md")) continue;
    const src = path.join(entry.dir, f);
    const st = fs.statSync(src);
    if (st.isDirectory()) {
      fs.mkdirSync(path.join(tmp, f));
      copyTree(src, path.join(tmp, f));
    } else if (st.isFile()) {
      fs.copyFileSync(src, path.join(tmp, f));
    }
  }
  return tmp;
}

async function deployCase(entry, deployCmd, timeoutMs, log) {
  const dir = stageCaseDir(entry);
  const env = {
    ...process.env,
    CASE_DIR: dir,
    CASE_ID: entry.id,
    CASE_HOSTNAME: resolveHostname(entry) ?? "",
  };
  await new Promise((resolve, reject) => {
    execFile(
      "sh",
      ["-c", deployCmd],
      { env, timeout: timeoutMs, maxBuffer: 8 * 1024 * 1024 },
      (error, stdout, stderr) => {
        if (error)
          reject(
            new Error(
              `deploy-cmd failed (${error.message})\n${String(stderr).slice(-2000)}`,
            ),
          );
        else {
          if (stdout.trim()) log(`  deploy: ${stdout.trim().split("\n").pop()}`);
          resolve();
        }
      },
    );
  });
  return dir;
}

/** Run one case end-to-end. Returns the case result record. */
export async function runCase(entry, opts) {
  const c = entry.case;
  const started = Date.now();
  const result = {
    id: entry.id,
    binding: entry.binding,
    title: c.title,
    status: "passed",
    steps: [],
  };
  if (opts.stub && c.requiresWorkerd !== false) {
    result.status = "skipped";
    result.reason = "requires workerd (stack is --stub)";
    return result;
  }
  const host = opts.host ?? resolveHostname(entry);
  if (!host) {
    result.status = "failed";
    result.reason = "no hostname (case.hostname/manifest missing, no --host)";
    return result;
  }
  if (opts.deployCmd) {
    try {
      await deployCase(entry, opts.deployCmd, opts.deployTimeoutMs, opts.log);
    } catch (e) {
      result.status = "failed";
      result.reason = `deploy failed: ${e.message}`;
      return result;
    }
  }
  const readyTimeout = c.readyTimeoutMs ?? opts.readyTimeoutMs;
  for (const [i, step] of c.steps.entries()) {
    // Step 0 doubles as the readiness gate: after a deploy the worker
    // takes a moment to converge, so it retries for the case's
    // readiness budget. Later steps only retry when they say so.
    const retry =
      step.retry ?? (i === 0 ? { timeoutMs: readyTimeout, intervalMs: 1500 } : null);
    const r = await runStepWithRetry(opts.url, host, step, opts.timeoutMs, retry);
    const rec = {
      name: step.name,
      status: r.ok ? "passed" : "failed",
    };
    if (!r.ok) {
      rec.failures = r.diffs;
      if (r.actual)
        rec.actual = { status: r.actual.status, body: truncate(r.actual.bodyText) };
      result.status = "failed";
    }
    result.steps.push(rec);
    if (!r.ok && !opts.keepGoing) break;
  }
  result.durationMs = Date.now() - started;
  return result;
}

// ── reports ─────────────────────────────────────────────────────────

export function summarize(results) {
  const totals = {
    passed: 0,
    failed: 0,
    skipped: 0,
    stepsPassed: 0,
    stepsFailed: 0,
  };
  for (const r of results) {
    totals[r.status === "passed" ? "passed" : r.status === "failed" ? "failed" : "skipped"]++;
    for (const s of r.steps ?? [])
      s.status === "passed" ? totals.stepsPassed++ : totals.stepsFailed++;
  }
  return totals;
}

export function buildReport({ url, stack, results }) {
  return {
    schemaVersion: SCHEMA_VERSION,
    generatedAt: new Date().toISOString(),
    url,
    stack,
    cases: results,
    totals: summarize(results),
  };
}

export function renderMarkdown(report) {
  const t = report.totals;
  const lines = [
    `# OpenFlare binding conformance — ${report.generatedAt}`,
    "",
    `Stack: \`${report.url}\` (${report.stack}) — ` +
      `**${t.passed} passed, ${t.failed} failed, ${t.skipped} skipped** ` +
      `(${t.stepsPassed}/${t.stepsPassed + t.stepsFailed} steps)`,
    "",
    "| case | binding | status | steps | reason |",
    "| --- | --- | --- | --- | --- |",
  ];
  for (const c of report.cases) {
    const steps = (c.steps ?? []).map((s) => (s.status === "passed" ? "ok" : "FAIL")).join(", ");
    lines.push(
      `| ${c.id} | ${c.binding} | ${c.status} | ${steps || "—"} | ${c.reason ?? ""} |`,
    );
  }
  const failed = report.cases.filter((c) => c.status === "failed");
  if (failed.length) {
    lines.push("", "## Failures", "");
    for (const c of failed) {
      lines.push(`### ${c.id}`);
      if (c.reason) lines.push(`- ${c.reason}`);
      for (const s of (c.steps ?? []).filter((s) => s.status === "failed")) {
        lines.push(`- step \`${s.name}\`:`);
        for (const f of s.failures ?? []) lines.push(`  - ${f}`);
      }
      lines.push("");
    }
  }
  return lines.join("\n") + "\n";
}

export function writeReport(report, resultsDir) {
  fs.mkdirSync(resultsDir, { recursive: true });
  const jsonPath = path.join(resultsDir, "latest.json");
  const mdPath = path.join(resultsDir, "latest.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2) + "\n");
  fs.writeFileSync(mdPath, renderMarkdown(report));
  return { jsonPath, mdPath };
}

// ── real-projects manifest ──────────────────────────────────────────

const EXPECT_STATES = ["works", "partial", "fails", "unknown"];
const COMMIT_RE = /^[0-9a-f]{40}$/;

/** Validate real-projects.json. Returns {ok, errors, projects}. */
export function validateProjects(raw) {
  const errors = [];
  if (!isRecord(raw)) return { ok: false, errors: ["manifest must be an object"], projects: [] };
  if (raw.schemaVersion !== SCHEMA_VERSION)
    err(errors, "schemaVersion", `must be ${SCHEMA_VERSION}`);
  if (!Array.isArray(raw.projects)) {
    err(errors, "projects", "required: array");
    return { ok: false, errors, projects: [] };
  }
  const seen = new Set();
  raw.projects.forEach((p, i) => {
    const at = `projects[${i}]`;
    if (!isRecord(p)) return err(errors, at, "must be an object");
    for (const k of Object.keys(p))
      if (!["name", "repo", "commit", "path", "bindings", "expect", "notes"].includes(k))
        err(errors, `${at}.${k}`, "unknown field");
    if (typeof p.name !== "string" || !p.name) err(errors, `${at}.name`, "required");
    else if (seen.has(p.name)) err(errors, `${at}.name`, `duplicate "${p.name}"`);
    else seen.add(p.name);
    if (typeof p.repo !== "string" || !/^https:\/\/[^/]+\/[^/]+\/[^/]+/.test(p.repo))
      err(errors, `${at}.repo`, "required: https repository URL");
    if (p.commit !== null && p.commit !== undefined && (typeof p.commit !== "string" || !COMMIT_RE.test(p.commit)))
      err(errors, `${at}.commit`, "must be a 40-hex commit sha or null");
    if (p.path !== null && p.path !== undefined && typeof p.path !== "string")
      err(errors, `${at}.path`, "must be a string or null");
    if (!Array.isArray(p.bindings) || !p.bindings.every((b) => typeof b === "string" && b))
      err(errors, `${at}.bindings`, "required: array of binding names");
    if (!EXPECT_STATES.includes(p.expect))
      err(errors, `${at}.expect`, `must be one of ${EXPECT_STATES.join("|")}`);
    if (p.notes !== null && p.notes !== undefined && typeof p.notes !== "string")
      err(errors, `${at}.notes`, "must be a string or null");
  });
  return { ok: errors.length === 0, errors, projects: raw.projects };
}

export function loadProjects(conformanceDir) {
  const p = path.join(conformanceDir, "real-projects.json");
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  return validateProjects(raw);
}

export function listProjects(raw) {
  const lines = raw.projects.map(
    (p) =>
      `${p.name}\t${p.bindings.join("+")}\t${p.expect}\t${p.repo}` +
      `${p.commit ? `@${p.commit.slice(0, 12)}` : ""}${p.notes ? `\t${p.notes}` : ""}`,
  );
  return lines.join("\n");
}

// ── CLI ─────────────────────────────────────────────────────────────

const USAGE = `usage: node conformance/run.mjs [options]

Run the binding-conformance cases against a running OpenFlare stack.

  --url <base>          ingress base URL (e.g. http://127.0.0.1:8080) [required]
  --deploy-cmd <shell>  command that deploys "$CASE_DIR" (the staged case
                        dir); env CASE_ID / CASE_HOSTNAME are also set
  --no-deploy           skip deploys; assert cases are already live
  --host <hostname>     Host header override for every request
  --stub                stack runs the stub runtime: requiresWorkerd
                        cases report "skipped"
  --only <regex>        run only case ids matching the regex
  --ready-timeout <ms>  per-case readiness budget after deploy (90000)
  --timeout <ms>        per-request timeout (15000)
  --deploy-timeout <ms> deploy command budget (180000)
  --fail-fast           stop the case at its first failed step
  --results <dir>       report output dir (conformance/results)
  --list                print the real-projects manifest and exit
  --validate            validate all manifests and exit
  -h, --help            this help
`;

function parseArgs(argv) {
  const o = {
    deployTimeoutMs: 180_000,
    readyTimeoutMs: 90_000,
    timeoutMs: 15_000,
    keepGoing: true,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const val = () => argv[++i];
    switch (a) {
      case "--url": o.url = val(); break;
      case "--deploy-cmd": o.deployCmd = val(); break;
      case "--no-deploy": o.noDeploy = true; break;
      case "--host": o.host = val(); break;
      case "--stub": o.stub = true; break;
      case "--only": o.only = new RegExp(val()); break;
      case "--ready-timeout": o.readyTimeoutMs = Number(val()); break;
      case "--timeout": o.timeoutMs = Number(val()); break;
      case "--deploy-timeout": o.deployTimeoutMs = Number(val()); break;
      case "--fail-fast": o.keepGoing = false; break;
      case "--results": o.resultsDir = val(); break;
      case "--list": o.list = true; break;
      case "--validate": o.validate = true; break;
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
  const projects = loadProjects(here);
  if (opts.list) {
    if (!projects.ok) {
      errlog(`real-projects.json invalid:\n  ${projects.errors.join("\n  ")}`);
      return 2;
    }
    log(listProjects(projects));
    return 0;
  }
  const entries = loadCases(path.join(here, "cases"));
  const bad = entries.filter((e) => !e.ok);
  for (const e of bad) for (const er of e.errors) errlog(`case ${e.id}: ${er}`);
  if (!projects.ok)
    for (const er of projects.errors) errlog(`real-projects.json: ${er}`);
  if (opts.validate) {
    const ok = bad.length === 0 && projects.ok;
    log(`${ok ? "OK" : "INVALID"}: ${entries.length} cases, ${projects.projects.length} real projects`);
    return ok ? 0 : 2;
  }
  if (bad.length || !projects.ok) return 2;
  if (!opts.url) {
    errlog("--url is required to run cases\n" + USAGE);
    return 2;
  }
  if (!opts.deployCmd && !opts.noDeploy) {
    errlog("pass --deploy-cmd <cmd> or --no-deploy (cases already live)\n" + USAGE);
    return 2;
  }
  const wanted = opts.only ? entries.filter((e) => opts.only.test(e.id)) : entries;
  if (wanted.length === 0) {
    errlog("no cases matched --only");
    return 2;
  }
  const stack = opts.stub ? "stub" : "workerd";
  log(`conformance: ${wanted.length} case(s), stack=${stack}, url=${opts.url}`);
  const results = [];
  for (const entry of wanted) {
    const r = await runCase(entry, {
      url: opts.url,
      host: opts.host,
      stub: opts.stub,
      deployCmd: opts.deployCmd,
      deployTimeoutMs: opts.deployTimeoutMs,
      readyTimeoutMs: opts.readyTimeoutMs,
      timeoutMs: opts.timeoutMs,
      keepGoing: opts.keepGoing,
      log,
    });
    const mark = r.status === "passed" ? "PASS" : r.status === "failed" ? "FAIL" : "SKIP";
    log(`${mark} ${entry.id}${r.reason ? ` (${r.reason})` : ""}`);
    results.push(r);
  }
  const report = buildReport({ url: opts.url, stack, results });
  const { jsonPath, mdPath } = writeReport(report, opts.resultsDir ?? path.join(here, "results"));
  const t = report.totals;
  log(`\n${t.passed} passed, ${t.failed} failed, ${t.skipped} skipped — ${jsonPath} + ${mdPath}`);
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
