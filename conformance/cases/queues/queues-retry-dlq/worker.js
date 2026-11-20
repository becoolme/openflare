// Retry/DLQ probe: the conf-q-retry consumer retries every message;
// each delivery appends its attempt number to attempts_log. The DLQ
// consumer appends to dlq_log. Both live in the same D1.
const DDL =
  "CREATE TABLE IF NOT EXISTS log (id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT, attempts INTEGER, body TEXT)";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    try {
      if (url.pathname === "/reset") {
        // Durable stores (D1 on CF and on a platform) outlive a
        // redeploy, so a rerun must start from an empty log.
        await env.DB.exec("DROP TABLE IF EXISTS log");
        return j({ ok: true });
      }
      if (url.pathname === "/send") {
        await env.Q.send("will-retry", { contentType: "text" });
        return j({ ok: true });
      }
      if (url.pathname === "/stats") {
        await env.DB.exec(DDL);
        const rows = await env.DB
          .prepare("SELECT lane, attempts, body FROM log ORDER BY id")
          .all();
        const main = rows.results.filter((r) => r.lane === "conf-q-retry");
        const dlq = rows.results.filter((r) => r.lane === "conf-q-dlq");
        return j({
          mainDeliveries: main.length,
          mainAttempts: main.map((r) => r.attempts),
          dlqDeliveries: dlq.length,
          dlqAttempts: dlq.map((r) => r.attempts),
          dlqBodies: dlq.map((r) => r.body),
        });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e) });
    }
  },

  async queue(batch, env) {
    await env.DB.exec(DDL);
    for (const m of batch.messages) {
      await env.DB.prepare(
        "INSERT INTO log (lane, attempts, body) VALUES (?, ?, ?)",
      )
        .bind(batch.queue, m.attempts, String(m.body))
        .run();
    }
    if (batch.queue === "conf-q-retry") {
      // Always retry — the message must be redelivered until it
      // exhausts max_retries, then dead-lettered.
      for (const m of batch.messages) m.retry();
    } else {
      batch.ackAll();
    }
  },
};
