// Conformance case: D1 dump() and raw() variants. env.DB is the real
// workerd D1Database (wrapped binding cloudflare-internal:d1-api →
// facade-d1 → provider → vrt-sql), so `dump()` and `raw()` are
// workerd's own client code and the export is produced by vrt-sql.
const SETUP = [
  "DROP TABLE IF EXISTS t",
  "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT, r REAL, b BLOB, n)",
  "INSERT INTO t VALUES (1, 'it''s', 1.5, X'00ff10', NULL)",
  "INSERT INTO t VALUES (2, 'line1' || char(10) || 'line2' || char(13) || 'end', 9e999, X'', 'x')",
  "INSERT INTO t VALUES (3, '', -9e999, NULL, 42)",
];

// 55 x ~1,000,000 chars: the serialized result crosses D1's documented
// 50 MB maximum query result size without storing anything.
const TOO_LARGE =
  "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<55) " +
  "SELECT x, replace(hex(zeroblob(1000000)), '00', 'a') AS s FROM c";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    try {
      if (url.pathname === "/reset") {
        // A D1 database outlives a redeploy: start from no user tables
        // so the empty-database dump assertion is deterministic.
        const r = await env.DB.exec("DROP TABLE IF EXISTS t");
        return j({ count: r.count });
      }
      if (url.pathname === "/dump-empty") {
        const buf = await env.DB.dump();
        const text = new TextDecoder().decode(buf);
        return j({ isArrayBuffer: buf instanceof ArrayBuffer, text });
      }
      if (url.pathname === "/setup") {
        const r = await env.DB.exec(SETUP.join("\n"));
        return j({ count: r.count });
      }
      if (url.pathname === "/raw") {
        const stmt = () => env.DB.prepare("SELECT id, s FROM t ORDER BY id");
        const def = await stmt().raw();
        const off = await stmt().raw({ columnNames: false });
        const on = await stmt().raw({ columnNames: true });
        // Duplicate column names must survive as separate columns
        // (workerd keeps rows as arrays, never keyed objects).
        const dup = await env.DB
          .prepare("SELECT a.id, b.id FROM t a JOIN t b ON b.id = a.id ORDER BY a.id LIMIT 1")
          .raw({ columnNames: true });
        const empty = await env.DB.prepare("SELECT id, s FROM t WHERE 0").raw();
        const emptyNames = await env.DB
          .prepare("SELECT id, s FROM t WHERE 0")
          .raw({ columnNames: true });
        return j({
          defaultRows: def.length,
          defaultWidths: def.map((r) => r.length),
          defaultFirst: def[0],
          falseMatchesDefault: JSON.stringify(def) === JSON.stringify(off),
          namesHeader: on[0],
          namesLength: on.length,
          namesFirstId: on[1][0],
          duplicateHeader: dup[0],
          duplicateRow: dup[1],
          emptyRows: empty,
          emptyWithNames: emptyNames,
        });
      }
      if (url.pathname === "/dump") {
        const buf = await env.DB.dump();
        const text = new TextDecoder().decode(buf);
        const lines = text.split("\n");
        // A dump is SQL: execute it back into the same database and
        // compare rows. This proves quoting/escaping, not just shapes.
        const select = "SELECT id, s, r, n, hex(b) AS h FROM t ORDER BY id";
        const before = await env.DB.prepare(select).raw();
        await env.DB.exec("DROP TABLE t");
        await env.DB.exec(text);
        const after = await env.DB.prepare(select).raw();
        return j({
          isArrayBuffer: buf instanceof ArrayBuffer,
          firstLine: lines[0],
          createLine: lines.find((l) => l.startsWith("CREATE TABLE")),
          insertCount: lines.filter((l) => l.startsWith("INSERT INTO")).length,
          row1: lines.find((l) => l.includes("VALUES(1,")),
          quoteEscaped: text.includes("'it''s'"),
          newlineEscaped: text.includes(
            "replace(replace('line1\\nline2\\rend','\\r',char(13)),'\\n',char(10))",
          ),
          inf: text.includes("9e999") && text.includes("-9e999"),
          blobHex: text.includes("X'00ff10'") && text.includes("X''"),
          roundTrip: JSON.stringify(before) === JSON.stringify(after),
          restoredRows: after.length,
        });
      }
      if (url.pathname === "/too-large") {
        let message = null;
        try {
          await env.DB.prepare(TOO_LARGE).all();
        } catch (e) {
          message = String(e?.message ?? e);
        }
        return j({ threw: message !== null, message });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e) });
    }
  },
};
