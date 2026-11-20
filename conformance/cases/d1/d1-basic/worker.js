// D1 conformance probe — env.DB is the real workerd D1Database
// (wrapped binding cloudflare-internal:d1-api → facade-d1 → provider
// → vrt-sql), so every documented method is exercised verbatim.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    try {
      if (url.pathname === "/setup") {
        const r = await env.DB.exec(
          "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT);",
        );
        return j({ count: r.count });
      }
      if (url.pathname === "/run") {
        const r = await env.DB.prepare("INSERT INTO t (v) VALUES (?)")
          .bind("alpha")
          .run();
        return j({
          success: r.success,
          changes: r.meta?.changes,
          lastRowIdIsInt: Number.isInteger(r.meta?.last_row_id),
          durationIsNumber: typeof r.meta?.duration === "number",
        });
      }
      if (url.pathname === "/all") {
        const r = await env.DB.prepare("SELECT v FROM t ORDER BY v").all();
        return j({
          success: r.success,
          values: r.results.map((row) => row.v),
          changesIsInt: Number.isInteger(r.meta?.changes),
        });
      }
      if (url.pathname === "/first") {
        const row = await env.DB.prepare("SELECT v FROM t ORDER BY v LIMIT 1").first();
        const scalar = await env.DB.prepare("SELECT v FROM t ORDER BY v LIMIT 1").first("v");
        const none = await env.DB.prepare("SELECT v FROM t WHERE 0").first();
        return j({ row, scalar, none });
      }
      if (url.pathname === "/raw") {
        const rows = await env.DB.prepare("SELECT id, v FROM t ORDER BY id LIMIT 2").raw();
        const withNames = await env.DB
          .prepare("SELECT id, v FROM t ORDER BY id LIMIT 1")
          .raw({ columnNames: true });
        return j({ rows, withNames });
      }
      if (url.pathname === "/batch") {
        const [ins, sel] = await env.DB.batch([
          env.DB.prepare("INSERT INTO t (v) VALUES (?)").bind("beta"),
          env.DB.prepare("SELECT v FROM t ORDER BY v"),
        ]);
        return j({
          insOk: ins.success,
          insChanges: ins.meta?.changes,
          selValues: sel.results.map((row) => row.v),
        });
      }
      if (url.pathname === "/batch-rollback") {
        // One bad statement rolls the whole batch back (atomic per
        // D1 semantics) — the first INSERT must not survive.
        let threw = false;
        try {
          await env.DB.batch([
            env.DB.prepare("INSERT INTO t (v) VALUES ('rollback-me')"),
            env.DB.prepare("SELEC oops"),
          ]);
        } catch (e) {
          threw = String(e?.message ?? e);
        }
        const row = await env.DB
          .prepare("SELECT v FROM t WHERE v = 'rollback-me'")
          .first();
        return j({ threw, rolledBack: row === null });
      }
      if (url.pathname === "/err") {
        const { sql, bind } = await request.json();
        try {
          const st = env.DB.prepare(sql);
          if (bind !== undefined) st.bind(...bind);
          await st.run();
          return j({ threw: false });
        } catch (e) {
          return j({ threw: true, message: String(e?.message ?? e), name: e.name });
        }
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e) });
    }
  },
};
