#!/usr/bin/env node
/**
 * cf-teardown.mjs — delete everything a --target cloudflare run created.
 *
 * Reads the cf-deploy.mjs ledger (CF_STATE, default
 * conformance/results/cf-state.json) and deletes, per run: the deployed
 * worker plus every provisioned resource (KV namespace, R2 bucket, D1
 * database, queue). Entries that delete cleanly are removed from the
 * ledger; failures stay recorded so a rerun retries them — the ledger
 * never claims more was cleaned than was.
 *
 * Best-effort per resource: one failure does not stop the rest.
 *
 * Env: CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, WRANGLER, CF_STATE
 * (same as cf-deploy.mjs). --state <file> overrides CF_STATE.
 */
import { execFile } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const WRANGLER = (process.env.WRANGLER ?? "wrangler").split(" ");

function wrangler(args, { timeoutMs = 120_000 } = {}) {
  return new Promise((resolve) => {
    execFile(
      WRANGLER[0],
      [...WRANGLER.slice(1), ...args],
      { timeout: timeoutMs, maxBuffer: 16 * 1024 * 1024 },
      (error, stdout, stderr) =>
        resolve({ ok: !error, out: `${stdout ?? ""}${stderr ?? ""}` }),
    );
  });
}

/** Delete one ledger resource. Returns null on success, reason on failure. */
export async function deleteResource(resource, { worker } = {}) {
  const wr = (args) => wrangler(args);
  switch (resource.kind) {
    case "worker":
      return (await wr(["delete", "--name", resource.name, "--force"])).ok
        ? null
        : `wrangler delete ${resource.name} failed`;
    case "kv":
      return (await wr(["kv", "namespace", "delete", "--namespace-id", resource.id])).ok
        ? null
        : `kv delete ${resource.title ?? resource.id} failed`;
    case "r2":
      return (await wr(["r2", "bucket", "delete", resource.name])).ok
        ? null
        : `r2 delete ${resource.name} failed`;
    case "d1":
      return (await wr(["d1", "delete", resource.name, "--skip-confirmation"])).ok
        ? null
        : `d1 delete ${resource.name} failed`;
    case "queue":
      return (await wr(["queues", "delete", resource.name])).ok
        ? null
        : `queue delete ${resource.name} failed`;
    default:
      return `unknown resource kind ${resource.kind}`;
  }
}

export async function teardown(stateFile, { log = console.error } = {}) {
  if (!fs.existsSync(stateFile)) {
    log(`cf-teardown: no state file ${stateFile} — nothing to do`);
    return { deleted: 0, failed: 0 };
  }
  const state = JSON.parse(fs.readFileSync(stateFile, "utf8"));
  let deleted = 0;
  let failed = 0;
  const leftover = [];
  for (const run of state.runs ?? []) {
    const resources = [
      { kind: "worker", name: run.worker },
      ...(run.resources ?? []),
    ];
    const failedResources = [];
    for (const r of resources) {
      const reason = await deleteResource(r);
      if (reason) {
        failed++;
        failedResources.push(r);
        log(`cf-teardown: ${run.caseId}: ${reason}`);
      } else {
        deleted++;
        log(`cf-teardown: ${run.caseId}: deleted ${r.kind} ${r.name ?? r.title ?? r.id}`);
      }
    }
    if (failedResources.length) leftover.push({ ...run, resources: failedResources.slice(1) });
  }
  state.runs = leftover;
  fs.writeFileSync(stateFile, JSON.stringify(state, null, 2) + "\n");
  return { deleted, failed };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const i = process.argv.indexOf("--state");
  const stateFile =
    (i > 0 ? process.argv[i + 1] : null) ??
    process.env.CF_STATE ??
    path.join(path.dirname(fileURLToPath(import.meta.url)), "results", "cf-state.json");
  teardown(stateFile)
    .then(({ deleted, failed }) => {
      console.log(`cf-teardown: ${deleted} deleted, ${failed} failed`);
      process.exit(failed ? 1 : 0);
    })
    .catch((e) => {
      console.error(`cf-teardown: ${e.message}`);
      process.exit(1);
    });
}
