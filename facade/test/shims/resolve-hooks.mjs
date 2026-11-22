// Module resolve hooks for `node --test` and the contract fixture
// generator (registered via register.mjs):
//  - `cloudflare:workers` → the test shim (workerd builtin, not embeddable)
//  - `cloudflare-internal:*` → hard failure (not part of the facade set)
// Tests import the PUBLISHED dist bytes (facade/dist/**) directly — no
// TypeScript stripping happens at test time; what runs under `node --test`
// is byte-identical to what the manifest addresses for workerd embed.
const SHIM_URL = new URL("./cloudflare-workers.mjs", import.meta.url).href;
const SOCKETS_SHIM_URL = new URL("./cloudflare-sockets.mjs", import.meta.url).href;

export function resolve(specifier, context, next) {
  if (specifier === "cloudflare:workers") {
    return { url: SHIM_URL, shortCircuit: true };
  }
  if (specifier === "cloudflare:sockets") {
    return { url: SOCKETS_SHIM_URL, shortCircuit: true };
  }
  if (specifier.startsWith("cloudflare-internal:")) {
    throw new Error(`unshimmed workerd internal specifier: ${specifier}`);
  }
  return next(specifier, context);
}

// `.wasm` specifiers → a module whose default export is the compiled
// `WebAssembly.Module`, mirroring workerd's `wasm = embed` semantics.
// (The hook thread can't share objects with the app realm, so the
// synthesized module reads the bytes itself.)
import { fileURLToPath } from "node:url";
export function load(url, context, next) {
  if (url.endsWith(".wasm")) {
    return {
      format: "module",
      shortCircuit: true,
      source: `import { readFileSync } from "node:fs";
export default new WebAssembly.Module(readFileSync(${JSON.stringify(fileURLToPath(url))}));`,
    };
  }
  return next(url, context);
}
