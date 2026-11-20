// Cron probe — scheduled() writes what the ScheduledController gave
// it; /hits reads the durable log.
const DDL =
  "CREATE TABLE IF NOT EXISTS cron_hits (id INTEGER PRIMARY KEY AUTOINCREMENT, cron TEXT, scheduled_ms INTEGER)";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/hits") {
      try {
        await env.DB.exec(DDL);
        const rows = await env.DB
          .prepare("SELECT cron, scheduled_ms FROM cron_hits ORDER BY id")
          .all();
        return Response.json({
          fired: rows.results.length > 0,
          // Only the FIRST hit is asserted — a slower run may have
          // ticked more than once, and that is still conformant.
          first: rows.results[0] ?? null,
          count: rows.results.length,
        });
      } catch (e) {
        return Response.json({ error: String(e?.message ?? e) }, { status: 500 });
      }
    }
    return Response.json({ ok: true });
  },

  async scheduled(controller, env) {
    await env.DB.exec(DDL);
    await env.DB.prepare(
      "INSERT INTO cron_hits (cron, scheduled_ms) VALUES (?, ?)",
    )
      .bind(
        String(controller.cron),
        controller.scheduledTime instanceof Date
          ? controller.scheduledTime.getTime()
          : typeof controller.scheduledTime === "number"
            ? controller.scheduledTime
            : 0,
      )
      .run();
  },
};
