#!/usr/bin/env node
// Run every config-sandbox case and summarize. Exit 1 if any case failed.
//
//   node spike/config-sandbox/verify.mjs --workerd /path/to/workerd
//   node spike/config-sandbox/verify.mjs --docker-image debian:12-slim \
//     --workerd /path/to/linux-workerd
//
// `--workerd` may also come from OF_WORKERD_BIN / VRT_WORKERD_BIN.
// `--bundler` likewise from OF_BUNDLER (default: of-bundler/target/debug).

import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const RUNNER = join(HERE, "run.mjs");

const args = process.argv.slice(2);
const pass = [];
const extra = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--workerd" || args[i] === "--bundler" || args[i] === "--docker-image") {
    extra.push(args[i], args[i + 1]);
    i++;
  } else if (args[i] === "--keep") {
    extra.push("--keep");
  } else {
    console.error(`unknown argument: ${args[i]}`);
    process.exit(2);
  }
}

const cases = ["parity", "fs-import", "network", "loop"];

function runCase(name) {
  return new Promise((ok) => {
    const child = spawn(process.execPath, [RUNNER, "--case", name, ...extra], {
      stdio: ["ignore", "pipe", "inherit"],
      env: { ...process.env, OF_CF_SANDBOX_TIMEOUT_MS: process.env.OF_CF_SANDBOX_TIMEOUT_MS ?? "6000" },
    });
    let out = "";
    child.stdout.on("data", (d) => (out += d));
    child.on("close", (code) => ok({ name, code, out }));
  });
}

for (const name of cases) {
  console.log(`\n===== ${name}`);
  const r = await runCase(name);
  process.stdout.write(r.out);
  pass.push({ name, ok: r.code === 0 });
  if (r.code !== 0) process.exitCode = 1;
}

console.log("\n===== summary");
for (const r of pass) console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name}`);
