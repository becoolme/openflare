#!/usr/bin/env node
/**
 * cf-deploy.mjs — deploy one staged conformance case to REAL Cloudflare.
 *
 * Called by conformance/run.mjs --deploy-cmd for `--target cloudflare`
 * runs. The runner exports CASE_DIR / CASE_ID / CASE_HOSTNAME; this hook
 * provisions the account-side resources the case's wrangler manifest
 * names (KV namespaces, R2 buckets, D1 databases, queues), rewrites the
 * manifest for workers.dev (routes to *.vrt.local cannot deploy on a
 * real account), runs `wrangler deploy`, and reports the per-case
 * request host back to the runner on stdout:
 *
 *   VRT_CASE_HOSTNAME=conf-<case>.<subdomain>.workers.dev
 *
 * Required env: CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID.
 * Optional env:
 *   WRANGLER   wrangler binary (default: "wrangler"; use e.g. "npx wrangler@4")
 *   CF_STATE   teardown ledger file (default: conformance/results/cf-state.json)
 *
 * Everything provisioned is appended to CF_STATE so cf-teardown.mjs can
 * delete exactly what a run created — nothing else in the account.
 *
 * Importable as a library: the manifest rewrite and JSONC parsing are
 * pure functions, covered by conformance/test/cf-deploy.test.mjs
 * without touching a real account.
 */
import { execFile } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

// ── JSONC ───────────────────────────────────────────────────────────

/** Strip // and block comments (outside strings) + trailing commas. */
export function stripJsonc(src) {
  let out = "";
  let inStr = false;
  let esc = false;
  for (let i = 0; i < src.length; i++) {
    const c = src[i];
    if (inStr) {
      out += c;
      if (esc) esc = false;
      else if (c === "\\") esc = true;
      else if (c === '"') inStr = false;
      continue;
    }
    if (c === '"') {
      inStr = true;
      out += c;
    } else if (c === "/" && src[i + 1] === "/") {
      while (i < src.length && src[i] !== "\n") i++;
      i--;
    } else if (c === "/" && src[i + 1] === "*") {
      i += 2;
      while (i < src.length && !(src[i] === "*" && src[i + 1] === "/")) i++;
      i++;
    } else {
      out += c;
    }
  }
  return out.replace(/,(\s*[}\]])/g, "$1");
}

export function loadManifest(dir) {
  for (const f of ["wrangler.jsonc", "wrangler.json"]) {
    const p = path.join(dir, f);
    if (fs.existsSync(p))
      return { file: p, config: JSON.parse(stripJsonc(fs.readFileSync(p, "utf8"))) };
  }
  throw new Error(
    `${dir}: no wrangler.jsonc/wrangler.json — cases with only a vrt.toml manifest ` +
      `cannot deploy to real Cloudflare (mark them "cloudflare": false in case.json)`,
  );
}

// ── manifest rewrite (pure) ─────────────────────────────────────────

/**
 * The resources a manifest needs provisioned on the account before
 * `wrangler deploy` will accept it: ids that are placeholders in the
 * checked-in case manifests.
 */
export function provisionPlan(config) {
  const plan = { kv: [], r2: [], d1: [], queues: [] };
  for (const b of config.kv_namespaces ?? [])
    plan.kv.push({ binding: b.binding, title: b.id });
  for (const b of config.r2_buckets ?? [])
    plan.r2.push({ binding: b.binding, name: b.bucket_name });
  for (const db of config.d1_databases ?? [])
    plan.d1.push({ binding: db.binding, name: db.database_name });
  for (const q of config.queues?.producers ?? [])
    plan.queues.push(q.queue);
  for (const q of config.queues?.consumers ?? [])
    plan.queues.push(q.queue);
  plan.queues = [...new Set(plan.queues)];
  return plan;
}

/**
 * Apply provisioned resource ids + workers.dev routing to the manifest.
 * `resolved` = {kv: {binding→id}, d1: {binding→uuid}} — R2/queues bind
 * by name, which the manifest already carries.
 */
export function applyProvisioned(config, resolved = {}) {
  const out = structuredClone(config);
  delete out.routes; // *.vrt.local routes cannot deploy on a real account
  out.workers_dev = true;
  for (const b of out.kv_namespaces ?? [])
    b.id = resolved.kv?.[b.binding] ?? b.id;
  for (const db of out.d1_databases ?? [])
    db.database_id = resolved.d1?.[db.binding] ?? db.database_id;
  return out;
}

/** Pull the workers.dev host out of `wrangler deploy` output. */
export function deployedHostname(deployOutput, workerName) {
  const m = deployOutput.match(/https:\/\/([a-z0-9-]+\.[a-z0-9-]+\.workers\.dev)/i);
  if (m) return m[1];
  const sub = process.env.CF_SUBDOMAIN;
  if (sub) return `${workerName}.${sub}.workers.dev`;
  throw new Error(
    "wrangler deploy did not print a workers.dev URL and CF_SUBDOMAIN is not set",
  );
}

// ── wrangler shell-outs ─────────────────────────────────────────────

const WRANGLER = (process.env.WRANGLER ?? "wrangler").split(" ");

function wrangler(args, { timeoutMs = 120_000 } = {}) {
  return new Promise((resolve, reject) => {
    execFile(
      WRANGLER[0],
      [...WRANGLER.slice(1), ...args],
      { timeout: timeoutMs, maxBuffer: 16 * 1024 * 1024 },
      (error, stdout, stderr) => {
        if (error)
          reject(new Error(`wrangler ${args[0]} failed: ${stderr?.trim() || error.message}`));
        else resolve(String(stdout));
      },
    );
  });
}

/** wrangler prints banner lines around its JSON; find the payload. */
function parseWranglerJson(out) {
  const start = out.search(/[[{]/);
  if (start < 0) throw new Error(`no JSON in wrangler output: ${out.slice(0, 200)}`);
  return JSON.parse(out.slice(start));
}

async function ensureKv(title) {
  const list = parseWranglerJson(await wrangler(["kv", "namespace", "list"]));
  const hit = (Array.isArray(list) ? list : []).find((ns) => ns.title === title);
  if (hit) return { id: hit.id, created: false };
  const created = parseWranglerJson(await wrangler(["kv", "namespace", "create", title, "--json"]));
  const id = created.namespace_id ?? created.id;
  if (!id) throw new Error(`kv create ${title}: no id in ${JSON.stringify(created)}`);
  return { id, created: true };
}

async function ensureR2(name) {
  try {
    await wrangler(["r2", "bucket", "create", name]);
    return { created: true };
  } catch (e) {
    if (/already exists/i.test(e.message)) return { created: false };
    throw e;
  }
}

async function ensureD1(name) {
  const list = parseWranglerJson(await wrangler(["d1", "list", "--json"]));
  const hit = (Array.isArray(list) ? list : []).find((d) => d.name === name);
  if (hit) return { id: hit.uuid ?? hit.database_id, created: false };
  const created = parseWranglerJson(await wrangler(["d1", "create", name, "--json"]));
  const id = created.uuid ?? created.database_id;
  if (!id) throw new Error(`d1 create ${name}: no id in ${JSON.stringify(created)}`);
  return { id, created: true };
}

async function ensureQueue(name) {
  const list = parseWranglerJson(await wrangler(["queues", "list", "--json"])).catch(() => []);
  const names = (Array.isArray(list) ? list : []).map((q) => q.queue_name ?? q.name);
  if (names.includes(name)) return { created: false };
  await wrangler(["queues", "create", name]);
  return { created: true };
}

// ── state ledger ────────────────────────────────────────────────────

function statePath() {
  return (
    process.env.CF_STATE ??
    path.join(path.dirname(fileURLToPath(import.meta.url)), "results", "cf-state.json")
  );
}

export function appendState(stateFile, entry) {
  const dir = path.dirname(stateFile);
  fs.mkdirSync(dir, { recursive: true });
  const state = fs.existsSync(stateFile)
    ? JSON.parse(fs.readFileSync(stateFile, "utf8"))
    : { runs: [] };
  state.runs.push(entry);
  fs.writeFileSync(stateFile, JSON.stringify(state, null, 2) + "\n");
}

// ── deploy ──────────────────────────────────────────────────────────

export async function deploy({
  caseDir = process.env.CASE_DIR,
  caseId = process.env.CASE_ID,
  log = console.error,
} = {}) {
  if (!caseDir || !caseId) throw new Error("CASE_DIR and CASE_ID are required");
  if (!process.env.CLOUDFLARE_API_TOKEN) throw new Error("CLOUDFLARE_API_TOKEN is required");
  if (!process.env.CLOUDFLARE_ACCOUNT_ID) throw new Error("CLOUDFLARE_ACCOUNT_ID is required");

  const { file, config } = loadManifest(caseDir);
  const plan = provisionPlan(config);
  const resolved = { kv: {}, d1: {} };
  const resources = [];

  for (const k of plan.kv) {
    const r = await ensureKv(k.title);
    resolved.kv[k.binding] = r.id;
    resources.push({ kind: "kv", id: r.id, title: k.title });
    log(`  cf: kv ${k.binding} → ${r.id}${r.created ? " (created)" : ""}`);
  }
  for (const b of plan.r2) {
    const r = await ensureR2(b.name);
    resources.push({ kind: "r2", name: b.name });
    log(`  cf: r2 ${b.name}${r.created ? " (created)" : ""}`);
  }
  for (const d of plan.d1) {
    const r = await ensureD1(d.name);
    resolved.d1[d.binding] = r.id;
    resources.push({ kind: "d1", id: r.id, name: d.name });
    log(`  cf: d1 ${d.binding} → ${r.id}${r.created ? " (created)" : ""}`);
  }
  for (const q of plan.queues) {
    const r = await ensureQueue(q);
    resources.push({ kind: "queue", name: q });
    log(`  cf: queue ${q}${r.created ? " (created)" : ""}`);
  }

  const patched = applyProvisioned(config, resolved);
  fs.writeFileSync(file, JSON.stringify(patched, null, 2) + "\n");
  const out = await wrangler(["deploy", "--config", file]);
  const hostname = deployedHostname(out, patched.name);

  appendState(statePath(), {
    at: new Date().toISOString(),
    caseId,
    worker: patched.name,
    hostname,
    resources,
  });
  return { hostname, worker: patched.name, resources };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  deploy()
    .then(({ hostname }) => console.log(`VRT_CASE_HOSTNAME=${hostname}`))
    .catch((e) => {
      console.error(`cf-deploy: ${e.message}`);
      process.exit(1);
    });
}
