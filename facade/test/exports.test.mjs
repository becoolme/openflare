// T25/L12 contract: every system-worker's MAIN module may only export
// handlers — workerd turns each named export into a service-map entry
// and hard-fails on non-handler values:
//   service vrt-egress: Incorrect type for map entry 'DENIED_HEADER':
//   the provided value is not of type 'function or ExportedHandler'.
// Constants/helpers MUST live in non-entry modules. This test imports
// the published dist bytes (same as workerd embeds) and enforces it.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import "./shims/register.mjs";
import { distModule } from "./helpers.mjs";

const manifest = JSON.parse(
  readFileSync(new URL("../manifest.json", import.meta.url), "utf8"),
);

function isHandlerValue(value) {
  if (typeof value === "function") return true; // class or fn handler
  if (value && typeof value === "object") {
    // ExportedHandler shape: at least one lifecycle method.
    return ["fetch", "connect", "scheduled", "queue", "tail", "test", "trace", "email"].some(
      (m) => typeof value[m] === "function",
    );
  }
  return false;
}

test("manifest main modules export only handlers (workerd service map)", async () => {
  for (const [service, worker] of Object.entries(manifest.workers)) {
    const mod = await import(distModule(worker.mainModule));
    const named = Object.entries(mod);
    assert.ok(named.length > 0, `${service}: main module exports nothing`);
    for (const [name, value] of named) {
      assert.ok(
        isHandlerValue(value),
        `${service}: named export '${name}' is not a function/ExportedHandler ` +
          `(got ${typeof value}) — workerd will reject the whole service`,
      );
    }
    // The declared entrypoint must be among the exports.
    assert.ok(
      named.some(([name]) => name === worker.entrypoint) ||
        typeof mod.default !== "undefined",
      `${service}: entrypoint ${worker.entrypoint} not exported`,
    );
  }
});
