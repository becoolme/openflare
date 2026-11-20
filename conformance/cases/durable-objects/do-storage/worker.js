// DO storage probe — the class is a `DurableObject` subclass so
// ctx.storage (kv + sql) is the real workerd surface.
import { DurableObject } from "cloudflare:workers";

export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // blockConcurrencyWhile gate — every request awaits this.ready,
    // so the init write is always visible before any fetch.
    this.ready = ctx.blockConcurrencyWhile(async () => {
      await ctx.storage.put("initialized", true);
    });
  }

  async fetch(req) {
    await this.ready;
    const url = new URL(req.url);
    const s = this.ctx.storage;
    const j = (v) => Response.json(v);
    if (url.pathname === "/reset") {
      // Storage outlives redeploys; clear what earlier runs left so
      // the counter and row-count steps start from zero. The
      // constructor-written "initialized" flag is kept on purpose.
      await s.delete(["n", "a", "b", "t1"]);
      s.sql.exec("DROP TABLE IF EXISTS t");
      return j({ ok: true });
    }
    if (url.pathname === "/init") {
      return j({ initialized: await s.get("initialized") });
    }
    if (url.pathname === "/inc") {
      const n = ((await s.get("n")) ?? 0) + 1;
      await s.put("n", n);
      return j({ n });
    }
    if (url.pathname === "/kv-misc") {
      await s.put({ a: 1, b: 2 });
      const got = await s.get(["a", "b", "missing"]);
      await s.delete(["a"]);
      const after = await s.get("a");
      const list = await s.list({ prefix: "b" });
      return j({
        gotA: got.get("a"),
        gotB: got.get("b"),
        gotMissing: got.get("missing") ?? null,
        afterDelete: after ?? null,
        listNames: [...list.keys()],
      });
    }
    if (url.pathname === "/sql") {
      s.sql.exec("CREATE TABLE IF NOT EXISTS t (k INTEGER)");
      s.sql.exec("INSERT INTO t VALUES (?)", 7);
      const cur = s.sql.exec("SELECT k FROM t ORDER BY k");
      const rows = [...cur].map((r) => r.k);
      // Raw scalar count — mirrors CF docs' sql.exec(...).one().
      const count = s.sql.exec("SELECT count(*) AS c FROM t").one().c;
      return j({ rows, count, rowsReadIsNum: typeof cur.rowsRead === "number" });
    }
    if (url.pathname === "/txn") {
      // txn atomicity: the second put throws → the first is rolled
      // back, matching storage.transaction semantics.
      let threw = false;
      try {
        await s.transaction(async () => {
          await s.put("t1", 1);
          await s.get("missing").then(() => {
            throw new Error("boom");
          });
        });
      } catch {
        threw = true;
      }
      return j({ threw, rolledBack: (await s.get("t1")) === undefined });
    }
    return j({ ok: true });
  }
}

export default {
  async fetch(req, env) {
    const stub = env.COUNTER.get(env.COUNTER.idFromName("one"));
    return stub.fetch(req);
  },
};
