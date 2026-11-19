// vrt-sql golden fixture 录制器（§9 兼容性门禁）。
// 离线；只用仓库 node_modules 的 miniflare。
// 运行：node vrt-sql/golden/capture.mjs
// 输出：vrt-sql/golden/fixtures/d1-golden.json（原始 HTTP status/body/头标志）
//
// 原理：getD1Database() 返回的对象上 `.fetcher` 是 d1-api wrapped
// binding 的 inner service binding，直接对其发 /query|/execute 即
// workerd→vrt-sql 的线上路径等价物（MF-Namespace 头指定库）。

import { createRequire } from "node:module";
import { writeFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
// 定位仓库内 miniflare（pnpm 布局）
// golden/ → vrt-sql → workers-runtime → tools → server → 仓库根
const ROOT = join(HERE, "..", "..", "..", "..", "..");
const { execSync } = await import("node:child_process");
// 钉死核实版本：与 doc/platform/workers-runtime-sql-storage.md §2 出处一致
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
  try { bodyJson = JSON.parse(text); } catch { /* 非 JSON 体原样存 */ }
  return {
    name, path,
    requestBody: body,
    status: r.status,
    bookmarkHeader: r.headers.get("x-cf-d1-session-commit-token") !== null,
    bodyJson: bodyJson ?? text,
  };
}

const cases = [];

// —— 建基数据（录进 fixture 供回放复现）——
const setup = [
  { path: "/query", body: { sql: "CREATE TABLE t(a INTEGER, b TEXT, c BLOB)", params: [] } },
  { path: "/query", body: [
    { sql: "INSERT INTO t VALUES(?,?,?)", params: [1, "x", [0, 1, 255]] },
    { sql: "INSERT INTO t VALUES(?,?,?)", params: [2, "y", null] },
  ] },
];
for (const s of setup) await rec("_setup", s.path, s.body);

// 1. 三种 resultsFormat + 非法值
for (const fmt of ["ARRAY_OF_OBJECTS", "ROWS_AND_COLUMNS", "NONE", "BOGUS"]) {
  cases.push(await rec(`format-${fmt}`, `/query?resultsFormat=${fmt}`,
    { sql: "SELECT a,b,c FROM t ORDER BY a", params: [] }));
}
// 缺省（不带参数）
cases.push(await rec("format-default", "/query",
  { sql: "SELECT a FROM t ORDER BY a", params: [] }));

// 2. 单对象 vs 数组
cases.push(await rec("single-object", "/query",
  { sql: "SELECT a FROM t WHERE a=?", params: [1] }));
cases.push(await rec("batch-array", "/query", [
  { sql: "SELECT a FROM t WHERE a=?", params: [1] },
  { sql: "SELECT b FROM t WHERE a=?", params: [2] },
]));

// 3. 参数类型
cases.push(await rec("params-types", "/query",
  { sql: "SELECT ? AS n, ? AS s, ? AS z, ? AS bb", params: [42, "hi", null, [222, 173]] }));

// 4. 空/注释语句
cases.push(await rec("empty-sql", "/query", { sql: "", params: [] }));
cases.push(await rec("comment-only", "/query", { sql: "-- just a comment", params: [] }));
cases.push(await rec("comment-then-select", "/query",
  { sql: "-- head\nSELECT 1 AS one", params: [] }));

// 5. 错误：语法 / 约束 / 缺表
cases.push(await rec("syntax-error", "/query", { sql: "SELEC bad", params: [] }));
cases.push(await rec("missing-table", "/query", { sql: "SELECT * FROM nope", params: [] }));
cases.push(await rec("constraint-error", "/query", [
  { sql: "CREATE TABLE uq(x UNIQUE)", params: [] },
  { sql: "INSERT INTO uq VALUES(1)", params: [] },
]));
// 上面建表/插入成功；重复 insert 触发约束错
cases.push(await rec("constraint-violation", "/query",
  { sql: "INSERT INTO uq VALUES(1)", params: [] }));

// 6. batch 中途失败 → 整批回滚（首错）
cases.push(await rec("batch-mid-fail", "/query", [
  { sql: "INSERT INTO t VALUES(99,'z',null)", params: [] },
  { sql: "SELECT * FROM missing_table", params: [] },
]));
// 验证回滚：99 不应存在
cases.push(await rec("batch-rollback-check", "/query",
  { sql: "SELECT a FROM t WHERE a=99", params: [] }));

// 7. /execute 路径 + exec 行切分等价物（客户端按 \n 切行发数组）
cases.push(await rec("execute-single", "/execute",
  { sql: "UPDATE t SET b='w' WHERE a=1", params: [] }));
const execText = "UPDATE t SET b='v' WHERE a=1;\nUPDATE t SET b='u' WHERE a=2;";
cases.push(await rec("execute-line-split", "/execute",
  execText.trim().split("\n").map((s) => ({ sql: s, params: null }))));

// 8. 畸形请求（schema 面）：sql 非字符串 / 顶层标量
cases.push(await rec("malformed-sql-type", "/query", { sql: 123 }));
cases.push(await rec("malformed-scalar", "/query", 42));

mkdirSync(join(HERE, "fixtures"), { recursive: true });
const out = { miniflareVersion: version, capturedAt: "2026-11-09", setup, cases };
writeFileSync(join(HERE, "fixtures", "d1-golden.json"), JSON.stringify(out, null, 2));
console.log(`captured ${cases.length} cases, miniflare ${version}`);
await mf.dispose();
