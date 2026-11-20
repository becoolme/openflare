// vrt-sql golden fixture recorder (section 9 compatibility gate).
// Offline; uses only miniflare from the repo node_modules.
// Run: node vrt-sql/golden/capture.mjs
// Output: vrt-sql/golden/fixtures/d1-golden.json (raw HTTP status/body/header flags)
//
// Principle: the `.fetcher` on the object returned by getD1Database() is the inner
// service binding of the d1-api wrapped binding; sending /query|/execute straight to it is
// the equivalent of the workerd->vrt-sql wire path (the MF-Namespace header selects the db).

import { createRequire } from "node:module";
import { writeFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
// Locate miniflare in the repo (pnpm layout)
// golden/ -> vrt-sql -> workers-runtime -> tools -> server -> repo root
const ROOT = join(HERE, "..", "..", "..", "..", "..");
const { execSync } = await import("node:child_process");
// Pinned, verified version: matches the source cited in doc/platform/workers-runtime-sql-storage.md section 2
const PIN = "miniflare@5.20260916.0-alpha_@types+node@22.20.1";
const pkgDir = join(ROOT, "node_modules/.pnpm", PIN, "node_modules/miniflare");
const { Miniflare } = await import(`file://${pkgDir}/dist/src/index.js`);
const version = JSON.parse(
  (await import("node:fs")).readFileSync(`${pkgDir}/package.json`, "utf8")
).version;

const mf = new Miniflare({
  workers: [{
    config: {
      type: "worker",
      name: "w0",
      compatibilityDate: "2025-09-16",
      manifest: {
        mainModule: "w0.js",
        modulesRoot: HERE,
        modules: { "w0.js": { type: "esm", contents: "export default { fetch() { return new Response('ok'); } }" } },
      },
      env: { DB: { type: "d1", id: "golden-db-0001" } },
    },
  }],
});
const db = await mf.getD1Database("DB");
const fetcher = db.fetcher;
if (!fetcher) throw new Error("db.fetcher unavailable — miniflare shape changed");

const H = { "content-type": "application/json", "MF-Namespace": "golden-db-0001" };

async function rec(name, path, body) {
  const r = await fetcher.fetch(`http://d1${path}`, {
    method: "POST", headers: H, body: JSON.stringify(body),
  });
  const text = await r.text();
  let bodyJson = null;
  try { bodyJson = JSON.parse(text); } catch { /* non-JSON body stored as-is */ }
  return {
    name, path,
    requestBody: body,
    status: r.status,
    bookmarkHeader: r.headers.get("x-cf-d1-session-commit-token") !== null,
    bodyJson: bodyJson ?? text,
  };
}

const cases = [];

// -- Seed data (recorded into the fixture so replay can reproduce it) --
const setup = [
  { path: "/query", body: { sql: "CREATE TABLE t(a INTEGER, b TEXT, c BLOB)", params: [] } },
  { path: "/query", body: [
    { sql: "INSERT INTO t VALUES(?,?,?)", params: [1, "x", [0, 1, 255]] },
    { sql: "INSERT INTO t VALUES(?,?,?)", params: [2, "y", null] },
  ] },
];
for (const s of setup) await rec("_setup", s.path, s.body);

// 1. Three resultsFormat values + an invalid value
for (const fmt of ["ARRAY_OF_OBJECTS", "ROWS_AND_COLUMNS", "NONE", "BOGUS"]) {
  cases.push(await rec(`format-${fmt}`, `/query?resultsFormat=${fmt}`,
    { sql: "SELECT a,b,c FROM t ORDER BY a", params: [] }));
}
// Default (no parameter)
cases.push(await rec("format-default", "/query",
  { sql: "SELECT a FROM t ORDER BY a", params: [] }));

// 2. Single object vs array
cases.push(await rec("single-object", "/query",
  { sql: "SELECT a FROM t WHERE a=?", params: [1] }));
cases.push(await rec("batch-array", "/query", [
  { sql: "SELECT a FROM t WHERE a=?", params: [1] },
  { sql: "SELECT b FROM t WHERE a=?", params: [2] },
]));

// 3. Parameter types
cases.push(await rec("params-types", "/query",
  { sql: "SELECT ? AS n, ? AS s, ? AS z, ? AS bb", params: [42, "hi", null, [222, 173]] }));

// 4. Empty / comment-only statements
cases.push(await rec("empty-sql", "/query", { sql: "", params: [] }));
cases.push(await rec("comment-only", "/query", { sql: "-- just a comment", params: [] }));
cases.push(await rec("comment-then-select", "/query",
  { sql: "-- head\nSELECT 1 AS one", params: [] }));

// 5. Errors: syntax / constraint / missing table
cases.push(await rec("syntax-error", "/query", { sql: "SELEC bad", params: [] }));
cases.push(await rec("missing-table", "/query", { sql: "SELECT * FROM nope", params: [] }));
cases.push(await rec("constraint-error", "/query", [
  { sql: "CREATE TABLE uq(x UNIQUE)", params: [] },
  { sql: "INSERT INTO uq VALUES(1)", params: [] },
]));
// The table/insert above succeeded; a duplicate insert triggers a constraint error
cases.push(await rec("constraint-violation", "/query",
  { sql: "INSERT INTO uq VALUES(1)", params: [] }));

// 6. Mid-batch failure -> whole batch rolled back (first error)
cases.push(await rec("batch-mid-fail", "/query", [
  { sql: "INSERT INTO t VALUES(99,'z',null)", params: [] },
  { sql: "SELECT * FROM missing_table", params: [] },
]));
// Verify rollback: 99 must not exist
cases.push(await rec("batch-rollback-check", "/query",
  { sql: "SELECT a FROM t WHERE a=99", params: [] }));

// 7. /execute path + exec line-splitting equivalent (client splits lines on \n and sends an array)
cases.push(await rec("execute-single", "/execute",
  { sql: "UPDATE t SET b='w' WHERE a=1", params: [] }));
const execText = "UPDATE t SET b='v' WHERE a=1;\nUPDATE t SET b='u' WHERE a=2;";
cases.push(await rec("execute-line-split", "/execute",
  execText.trim().split("\n").map((s) => ({ sql: s, params: null }))));

// 8. Malformed requests (schema surface): non-string sql / top-level scalar
cases.push(await rec("malformed-sql-type", "/query", { sql: 123 }));
cases.push(await rec("malformed-scalar", "/query", 42));

mkdirSync(join(HERE, "fixtures"), { recursive: true });
const out = { miniflareVersion: version, capturedAt: "2026-11-09", setup, cases };
writeFileSync(join(HERE, "fixtures", "d1-golden.json"), JSON.stringify(out, null, 2));
console.log(`captured ${cases.length} cases, miniflare ${version}`);
await mf.dispose();
