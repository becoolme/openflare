export default {
  async fetch(request, env) {
    const url = new URL(request.url)
    if (url.pathname === '/api/hits') {
      await env.DB.exec(
        'CREATE TABLE IF NOT EXISTS cron_hits (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL, last TEXT)'
      )
      const row = await env.DB.prepare('SELECT n, last FROM cron_hits WHERE id = 1').first()
      return new Response(JSON.stringify({ hits: row ? row.n : 0, last: row ? row.last : null }), {
        headers: { 'content-type': 'application/json' },
      })
    }
    return new Response(JSON.stringify({ ok: true }), {
      headers: { 'content-type': 'application/json' },
    })
  },

  async scheduled(controller, env, ctx) {
    // Real scheduled event — every firing increments the durable
    // counter and records the controller surface we were handed.
    await env.DB.exec(
      'CREATE TABLE IF NOT EXISTS cron_hits (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL, last TEXT)'
    )
    await env.DB.prepare(
      'INSERT INTO cron_hits (id, n, last) VALUES (1, 1, ?) ON CONFLICT(id) DO UPDATE SET n = n + 1, last = ?'
    )
      .bind(`${controller.cron}@${controller.scheduledTime}`, `${controller.cron}@${controller.scheduledTime}`)
      .run()
  },
}
