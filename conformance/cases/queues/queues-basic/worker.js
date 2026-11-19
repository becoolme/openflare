// Queue producer+consumer probe. Delivered batches land in D1
// (id, queue, count, per-message fields normalized for determinism).
const DDL =
  "CREATE TABLE IF NOT EXISTS deliveries (id INTEGER PRIMARY KEY AUTOINCREMENT, queue TEXT, count INTEGER, info TEXT)";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    try {
      if (url.pathname === "/send") {
        await env.Q.send({ t: "hello" }, { contentType: "json" });
        return j({ ok: true });
      }
      if (url.pathname === "/send-batch") {
        await env.Q.sendBatch([
          { body: "m1", contentType: "text" },
          { body: "m2", contentType: "text" },
        ]);
        return j({ ok: true });
      }
      if (url.pathname === "/send-delay") {
        await env.Q.send("delayed-msg", {
          contentType: "text",
          delaySeconds: 2,
        });
        return j({ ok: true });
      }
      if (url.pathname === "/send-err") {
        // Over the 128,000-byte message ceiling — the producer
        // binding must reject it (provider enforces the documented
        // per-message cap).
        try {
          await env.Q.send("x".repeat(129_000), { contentType: "text" });
          return j({ threw: false });
        } catch (e) {
          return j({ threw: true, message: String(e?.message ?? e) });
        }
      }
      if (url.pathname === "/deliveries") {
        await env.DB.exec(DDL);
        const rows = await env.DB
          .prepare("SELECT id, queue, count, info FROM deliveries ORDER BY id")
          .all();
        return j({
          count: rows.results.length,
          deliveries: rows.results.map((r) => ({
            ...JSON.parse(r.info),
            queue: r.queue,
            count: r.count,
          })),
        });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e) });
    }
  },

  async queue(batch, env) {
    await env.DB.exec(DDL);
    const info = {
      queue: batch.queue,
      count: batch.messages.length,
      messages: batch.messages.map((m) => ({
        idIsString: typeof m.id === "string" && m.id.length > 0,
        tsIsNumber: m.timestamp instanceof Date && m.timestamp.getTime() > 0,
        attempts: m.attempts,
        body: m.body,
        delaySecs:
          m.timestamp instanceof Date
            ? Math.round((Date.now() - m.timestamp.getTime()) / 1000)
            : null,
      })),
    };
    await env.DB.prepare(
      "INSERT INTO deliveries (queue, count, info) VALUES (?, ?, ?)",
    )
      .bind(batch.queue, batch.messages.length, JSON.stringify(info))
      .run();
    batch.ackAll();
  },
};
