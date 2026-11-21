// sendBatch atomicity + max_concurrency probe.
//
// SINK (conf-q-sink) has no consumer: metrics().backlogCount tells us
// exactly what a producer call persisted, without a consumer draining
// it. A sendBatch whose second item is over the documented 128 KB
// per-message limit must reject the call and leave the backlog
// unchanged ("when the promise resolves, the messages are confirmed to
// be written to disk"; a rejected call writes nothing).
//
// conf-q-conc delivers one message per invocation with max_concurrency
// 1. Each invocation records its start/end in D1 after a fixed sleep,
// so the runner can assert that no two invocations overlap.
const DDL =
  "CREATE TABLE IF NOT EXISTS invocations (id INTEGER PRIMARY KEY AUTOINCREMENT, started INTEGER, ended INTEGER, body TEXT)";

const HANDLER_SLEEP_MS = 2000;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    try {
      if (url.pathname === "/reset") {
        // Durable stores outlive a redeploy; a rerun starts clean.
        await env.DB.exec("DROP TABLE IF EXISTS invocations");
        return j({ ok: true });
      }
      if (url.pathname === "/batch-partial") {
        // One valid item followed by one item over the 128,000-byte
        // limit. sendBatch has a single promise: it either writes the
        // whole batch or throws.
        const before = await env.SINK.metrics();
        let threw = false;
        let message = "";
        try {
          await env.SINK.sendBatch([
            { body: "valid-before-oversize", contentType: "text" },
            { body: "x".repeat(129_000), contentType: "text" },
          ]);
        } catch (e) {
          threw = true;
          message = String(e?.message ?? e);
        }
        const after = await env.SINK.metrics();
        return j({
          threw,
          before: before.backlogCount,
          after: after.backlogCount,
          // The valid item must not be left behind by a partially
          // accepted batch. Compared as a delta so reruns (the sink is
          // not drained) keep the same meaning.
          unchanged: after.backlogCount === before.backlogCount,
          message,
        });
      }
      if (url.pathname === "/batch-valid") {
        // Control: the same producer writes a legal two-item batch.
        const before = await env.SINK.metrics();
        await env.SINK.sendBatch([
          { body: "v1", contentType: "text" },
          { body: "v2", contentType: "text" },
        ]);
        const after = await env.SINK.metrics();
        return j({ delta: after.backlogCount - before.backlogCount });
      }
      if (url.pathname === "/send-conc") {
        // Three messages, delivered one per invocation (max_batch_size
        // 1) to a consumer capped at max_concurrency 1.
        await env.Q.sendBatch([
          { body: "c0", contentType: "text" },
          { body: "c1", contentType: "text" },
          { body: "c2", contentType: "text" },
        ]);
        return j({ ok: true });
      }
      if (url.pathname === "/stats") {
        await env.DB.exec(DDL);
        const rows = await env.DB
          .prepare("SELECT started, ended, body FROM invocations ORDER BY id")
          .all();
        const invocations = rows.results.map((r) => ({
          start: Number(r.started),
          end: Number(r.ended),
        }));
        // Max overlap probed at every interval start (half-open
        // [start,end)); serialized invocations never overlap.
        let maxOverlap = 0;
        for (const probe of invocations) {
          let active = 0;
          for (const iv of invocations) {
            if (iv.start <= probe.start && probe.start < iv.end) active += 1;
          }
          if (active > maxOverlap) maxOverlap = active;
        }
        return j({
          deliveries: rows.results.length,
          maxOverlap,
          bodies: [...new Set(rows.results.map((r) => String(r.body)))].sort(),
        });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e) });
    }
  },

  async queue(batch, env) {
    const started = Date.now();
    // Hold the invocation open long enough that a second delivery with
    // max_concurrency ignored would visibly overlap (the standalone
    // dispatcher ticks once per second).
    await new Promise((resolve) => setTimeout(resolve, HANDLER_SLEEP_MS));
    const ended = Date.now();
    await env.DB.exec(DDL);
    for (const m of batch.messages) {
      await env.DB.prepare(
        "INSERT INTO invocations (started, ended, body) VALUES (?, ?, ?)",
      )
        .bind(started, ended, String(m.body))
        .run();
    }
    batch.ackAll();
  },
};
