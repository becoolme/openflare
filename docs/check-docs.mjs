#!/usr/bin/env node
// Docs consistency checker — pure Node, no deps.
//
// 1. Every docs/bindings/<name>.md must carry the fixed section
//    structure (title, one-line status, the seven sections).
// 2. Every repo-relative path referenced in a binding page's
//    "Examples and tests" section (and inside backticks elsewhere in
//    docs/) must exist.
// 3. Support tiers are only Drop-in / Partial / Unsupported — the
//    binding page `**Status:` line must match the overview table row.
// 4. Platform codes referenced in docs ("platform code `x`" or
//    "code `x`") must be documented in docs/log-codes.md — the same
//    consistency rule the vrt-host tests enforce.

import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const docsDir = join(root, "docs");
const bindingsDir = join(docsDir, "bindings");

let failures = 0;
const fail = (file, msg) => {
  failures++;
  console.error(`FAIL ${file}: ${msg}`);
};

const TIERS = ["Drop-in", "Partial", "Unsupported"];
const REQUIRED_SECTIONS = [
  "## Wrangler configuration",
  "## Where it runs",
  "## Backends",
  "## Known differences from Cloudflare",
  "## Unsupported keys and error shape",
  "## Examples and tests",
];

// Repo-relative path prefixes worth existence-checking.
const PATH_RE =
  /`((?:examples|vrt-[a-z]+|cell-controller|facade|snapshot-protocol|shared|third_party|deploy|docs|site)\/[\w./-]+?)`/g;

function checkPaths(file, text) {
  for (const m of text.matchAll(PATH_RE)) {
    const p = m[1].replace(/:.*$/, "").replace(/[.,;:]+$/, "");
    if (!existsSync(join(root, p))) fail(file, `referenced path does not exist: ${p}`);
  }
}

// ---- 1 + 3: binding pages ----
const readme = readFileSync(join(bindingsDir, "README.md"), "utf8");
const readmeRows = new Map();
for (const m of readme.matchAll(/\|\s*\[([^\]]+)\]\(([\w.-]+)\)\s*\|\s*([\w-]+)\s*\|/g)) {
  readmeRows.set(m[2], m[3]);
}
if (/\bPluggable\b/.test(readme.replace(/no "pluggable" tier|pluggable provider/gi, ""))) fail("docs/bindings/README.md", "must not use the Pluggable tier");
for (const m of readme.matchAll(/\|\s*[^\n]*?\|\s*(\w[\w-]*)\s*\|\s*[^\n]*\|/g)) {
  // catch any status column not in the tier list (best-effort)
  if (!TIERS.includes(m[1]) && m[1] !== "Status" && !/^[-\s]+$/.test(m[1])) {
    // rows like `Workers AI (ai)` have non-link first cells; their tier is col 2 —
    // check every cell that looks like a tier position instead, below.
  }
}
// Stricter: every data row's second column must be a tier.
for (const line of readme.split("\n")) {
  if (!line.startsWith("|") || /^\|\s*-/.test(line) || /Status/.test(line) && /Why/.test(line)) continue;
  const cells = line.split("|").map((c) => c.trim()).filter(Boolean);
  if (cells.length < 2) continue;
  if (cells[0] === "Binding" || cells[0].startsWith("-")) continue;
  if (!TIERS.includes(cells[1])) {
    fail("docs/bindings/README.md", `row for ${cells[0]} has non-tier status "${cells[1]}"`);
  }
}

const pages = readdirSync(bindingsDir).filter((f) => f.endsWith(".md") && f !== "README.md");
for (const page of pages) {
  const file = `docs/bindings/${page}`;
  const text = readFileSync(join(bindingsDir, page), "utf8");
  if (!/^# \S/m.test(text)) fail(file, "missing `# Title` heading");
  const status = text.match(/^\*\*Status:\s*([\w-]+)\*\*/m);
  if (!status) {
    fail(file, "missing one-line `**Status: <tier>**` line");
  } else {
    if (!TIERS.includes(status[1])) fail(file, `invalid status tier "${status[1]}"`);
    const row = readmeRows.get(page);
    if (!row) {
      fail(file, `no row in docs/bindings/README.md links to ${page}`);
    } else if (row !== status[1]) {
      fail(file, `status "${status[1]}" does not match overview table "${row}"`);
    }
  }
  for (const sec of REQUIRED_SECTIONS) {
    if (!text.includes(sec)) fail(file, `missing section "${sec}"`);
  }
  checkPaths(file, text);
}

// Overview table must link every page.
for (const page of pages) {
  if (!readmeRows.has(page)) {
    fail("docs/bindings/README.md", `overview table missing a row linking ${page}`);
  }
}

// ---- 2: paths in top-level docs pages ----
for (const f of ["wrangler-config.md", "compatibility.md", "cli-reference.md"]) {
  const p = join(docsDir, f);
  if (!existsSync(p)) {
    fail(`docs/${f}`, "file missing");
    continue;
  }
  checkPaths(`docs/${f}`, readFileSync(p, "utf8"));
}

// ---- 4: platform codes must be documented in log-codes.md ----
const logCodes = readFileSync(join(docsDir, "log-codes.md"), "utf8");
const docFiles = [
  join(docsDir, "compatibility.md"),
  join(docsDir, "wrangler-config.md"),
  ...pages.map((p) => join(bindingsDir, p)),
];
for (const p of docFiles) {
  const text = readFileSync(p, "utf8");
  for (const m of text.matchAll(/(?:platform code|codes?)\s+`([a-z][a-z0-9_]*)`/g)) {
    if (!logCodes.includes("`" + m[1] + "`") && !logCodes.includes(m[1])) {
      fail(p.replace(root + "/", ""), `platform code "${m[1]}" not in docs/log-codes.md`);
    }
  }
}

if (failures) {
  console.error(`\n${failures} docs check failure(s)`);
  process.exit(1);
}
console.log(`docs check ok — ${pages.length} binding pages verified`);
