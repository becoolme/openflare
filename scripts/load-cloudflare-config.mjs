// load-cloudflare-config.mjs -- evaluate a `cloudflare.config.ts`/`.js` file
// with Cloudflare's own loader and print the wrangler-shaped config as JSON.
//
//   node scripts/load-cloudflare-config.mjs <configPath> [--mode M] [--preview]
//
// stdout (exit 0):
//   {"raw": <wrangler RawConfig>, "dependencies": ["<abs path>", ...]}
//
// stderr (single-line JSON) + non-zero exit:
//   2  the config did not parse against CloudflareConfigSchema
//   3  loading/evaluating the config failed (bad import, unsupported Node)
//   64 invalid command line (never seen by vrt itself)
//
// The caller (`vrt-local/src/project_cfg.rs`) runs this as a restricted
// child process: fixed cwd, cleared environment allowlist, timeout, and a
// stdout cap. This script never logs the config's values -- zod issues are
// projected to `code`/`path`/`message` only, and messages are truncated.
//
// `@cloudflare/config` must be resolvable from the config file's directory
// (i.e. installed in the project, like the `cf` CLI expects). Entrypoints
// imported with `with { type: "cf-worker" }` are turned into a synthetic
// module exporting the specifier -- the referenced module is never loaded
// or executed here.

import { readFileSync, writeSync } from "node:fs";
import { extname, isAbsolute, resolve } from "node:path";

const MAX_CONFIG_BYTES = 256 * 1024;
const MAX_ISSUES = 20;
const MAX_MESSAGE_CHARS = 500;
const CONFIG_EXTENSIONS = new Set([".ts", ".mts", ".cts", ".js", ".mjs", ".cjs"]);
// module.registerHooks() landed in Node 22.15; the helper fails closed on
// anything older instead of silently falling back to an unsupported path.
const MIN_NODE = [22, 15];

// stdout/stderr are read by vrt's Rust side, so they must be fully written
// before exit: `process.exit()` can drop queued stream writes, while
// `writeSync` cannot.
function writeOut(text) {
  writeSync(1, text);
}

function writeErr(text) {
  writeSync(2, text);
}

function emitError(error, message, issues) {
  const payload = { error, message: truncate(message) };
  if (issues && issues.length > 0) payload.issues = issues;
  writeErr(`${JSON.stringify(payload)}\n`);
}

function truncate(message) {
  const text = String(message ?? "").replace(/\s+/g, " ").trim();
  const chars = Array.from(text);
  return chars.length > MAX_MESSAGE_CHARS
    ? `${chars.slice(0, MAX_MESSAGE_CHARS).join("")}...`
    : text;
}

function usage(message) {
  emitError("VRT_CONFIG_LOADER_USAGE", message);
  process.exit(64);
}

function parseArgs(argv) {
  let configPath;
  let mode;
  let isPreview = false;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--mode") {
      const value = argv[++i];
      if (value === undefined || value === "" || value.startsWith("--")) {
        usage("--mode requires a value");
      }
      mode = value;
    } else if (arg === "--preview") {
      isPreview = true;
    } else if (arg.startsWith("--")) {
      usage(`unknown option ${arg}`);
    } else if (configPath === undefined) {
      configPath = arg;
    } else {
      usage("exactly one config path is accepted");
    }
  }
  if (configPath === undefined) usage("config path is required");
  return { configPath, mode, isPreview };
}

/** Zod issues -> stable, value-free diagnostics. */
function projectIssues(error) {
  const issues = Array.isArray(error?.issues) ? error.issues : [];
  const projected = issues.slice(0, MAX_ISSUES).map((issue) => {
    const path = Array.isArray(issue?.path) ? issue.path.join(".") : "";
    const message = truncate(issue?.message ?? issue?.code ?? "invalid");
    return path
      ? { path, code: String(issue?.code ?? ""), message }
      : { code: String(issue?.code ?? ""), message };
  });
  if (issues.length > MAX_ISSUES) {
    projected.push({ code: "too_many_issues", message: `${issues.length - MAX_ISSUES} more not shown` });
  }
  return projected;
}

function nodeIsSupported() {
  const [major, minor] = process.versions.node.split(".").map(Number);
  return major > MIN_NODE[0] || (major === MIN_NODE[0] && minor >= MIN_NODE[1]);
}

async function main() {
  const { configPath, mode, isPreview } = parseArgs(process.argv.slice(2));
  const absolute = isAbsolute(configPath) ? configPath : resolve(process.cwd(), configPath);

  const extension = extname(absolute).toLowerCase();
  if (!CONFIG_EXTENSIONS.has(extension)) {
    usage(`unsupported config extension ${extension || "<none>"}`);
  }
  let size;
  try {
    size = readFileSync(absolute).byteLength;
  } catch (error) {
    usage(`cannot read config file: ${error?.code ?? error?.message ?? "unknown error"}`);
  }
  if (size > MAX_CONFIG_BYTES) {
    usage(`config file exceeds ${MAX_CONFIG_BYTES} bytes`);
  }
  if (!nodeIsSupported()) {
    emitError(
      "VRT_CONFIG_LOADER_FAILED",
      `cloudflare.config evaluation needs Node >= ${MIN_NODE.join(".")} (module.registerHooks); running ${process.versions.node}`,
    );
    process.exit(3);
  }

  let api;
  try {
    api = await import("@cloudflare/config");
  } catch (error) {
    emitError(
      "VRT_CONFIG_LOADER_FAILED",
      `@cloudflare/config is not resolvable from ${absolute} -- install it in the project (${error?.code ?? error?.message ?? "import failed"})`,
    );
    process.exit(3);
  }

  try {
    api.registerConfigHooks();
  } catch (error) {
    emitError("VRT_CONFIG_LOADER_FAILED", `registerConfigHooks failed: ${error?.message ?? error}`);
    process.exit(3);
  }

  let loaded;
  try {
    loaded = await api.loadAndParseConfig(absolute, { mode, isPreview });
  } catch (error) {
    emitError("VRT_CONFIG_LOADER_FAILED", `loading the config failed: ${error?.message ?? error}`);
    process.exit(3);
  }

  if (!loaded?.result?.success) {
    const issues = projectIssues(loaded?.result?.error);
    const first = issues[0];
    emitError(
      "VRT_CONFIG_INVALID",
      first?.path ? `${first.path}: ${first.message}` : (first?.message ?? "config did not parse"),
      issues,
    );
    process.exit(2);
  }

  let raw;
  try {
    raw = api.convertToWranglerConfig(loaded.result.data);
  } catch (error) {
    emitError("VRT_CONFIG_LOADER_FAILED", `convertToWranglerConfig failed: ${error?.message ?? error}`);
    process.exit(3);
  }

  // Sorted for deterministic output: vrt ignores the list today, but a
  // stable order keeps the helper's contract testable.
  const dependencies = [...(loaded.dependencies ?? [])].map(String).sort();
  writeOut(`${JSON.stringify({ raw, dependencies })}\n`);
}

main().catch((error) => {
  emitError("VRT_CONFIG_LOADER_FAILED", `unexpected loader failure: ${error?.message ?? error}`);
  process.exit(3);
});
