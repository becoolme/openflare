// Queue producer+consumer probe. Delivered batches land in D1
// (id, queue, count, per-message fields normalized for determinism).
const DDL =
  "CREATE TABLE IF NOT EXISTS deliveries (id INTEGER PRIMARY KEY AUTOINCREMENT, queue TEXT, count INTEGER, info TEXT)";

// Docs: QueueSendOptions.delaySeconds "must be an integer between 0 and
// 86400 (24 hours)". A value outside the range is refused by the
// producer binding (the provider re-validates the wire header), so the
// probe only reports whether the call threw.
async function sendDelay(env, body, delaySeconds) {
  try {
    const result = await env.Q.send(body, { contentType: "text", delaySeconds });
    return { threw: false, result };
  } catch (e) {
    return { threw: true, message: String(e?.message ?? e) };
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    try {
      if (url.pathname === "/reset") {
        // Durable stores (D1 on CF and on a platform) outlive a
        // redeploy, so a rerun must start from an empty log.
        await env.DB.exec("DROP TABLE IF EXISTS deliveries");
        return j({ ok: true });
      }
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
        const r = await sendDelay(env, "delayed-msg", 5);
        return j({ ok: !r.threw, threw: r.threw });
      }
      if (url.pathname === "/send-delay-neg") {
        // Below the documented floor (0).
        const r = await sendDelay(env, "neg-msg", -1);
        return j({ threw: r.threw });
      }
      if (url.pathname === "/send-delay-over") {
        // Above the documented ceiling (86 400 s).
        const r = await sendDelay(env, "over-msg", 86_401);
        return j({ threw: r.threw });
      }
      if (url.pathname === "/send-zero") {
        // 0 is the floor and explicitly means "do not delay".
        const r = await sendDelay(env, "immediate-msg", 0);
        const metrics = r.result?.metadata?.metrics;
        return j({
          threw: r.threw,
          // QueueSendResult.metadata.metrics is part of the documented
          // send() resolution value; the numeric counters must be numbers.
          hasMetadata: typeof r.result?.metadata === "object" && r.result.metadata !== null,
          backlogIsNumber: typeof metrics?.backlogCount === "number",
          backlogBytesIsNumber: typeof metrics?.backlogBytes === "number",
        });
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
      if (url.pathname === "/delays") {
        // Normalized view of the two delay probes. Delivery order is
        // best effort, so the rows are searched by body instead of by
        // position, and the wall-clock delay is reduced to booleans
        // (a tight upper bound would be CI-noise flaky).
        await env.DB.exec(DDL);
        const rows = await env.DB
          .prepare("SELECT info FROM deliveries ORDER BY id")
          .all();
        const seen = new Map();
        for (const row of rows.results) {
          for (const m of JSON.parse(row.info).messages ?? []) {
            if (typeof m.delaySecs === "number") seen.set(m.body, m.delaySecs);
          }
        }
        const delayed = seen.get("delayed-msg");
        const immediate = seen.get("immediate-msg");
        return j({
          delayedFound: typeof delayed === "number",
          immediateFound: typeof immediate === "number",
          // Message.timestamp is "when the message was sent", so a
          // message sent with delaySeconds: 5 must be delivered at least
          // 5 s later. 4 leaves a second of clock slack between the
          // data-plane send stamp and the worker-side Date.now(); the
          // observed baseline (an undelayed message on the same 1 s
          // dispatch cadence) is ~2 s, so a delay-ignoring
          // implementation would still fail.
          delayedHonored: typeof delayed === "number" && delayed >= 4,
        });
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
