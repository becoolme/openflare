export default {
  async fetch(request, env) {
    const url = new URL(request.url)
    if (url.pathname === '/send') {
      // Producer path — the T37 queue binding POSTs to vrt-sql.
      await env.Q.send({ sent: Date.now() })
      return new Response(JSON.stringify({ ok: true }), {
        headers: { 'content-type': 'application/json' },
      })
    }
    if (url.pathname === '/api/hits') {
      try {
        await env.DB.exec(
          'CREATE TABLE IF NOT EXISTS queue_hits (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL)'
        )
        const row = await env.DB.prepare('SELECT n FROM queue_hits WHERE id = 1').first()
        return new Response(JSON.stringify({ hits: row ? row.n : 0 }), {
          headers: { 'content-type': 'application/json' },
        })
      } catch (e) {
        return new Response(JSON.stringify({ error: String((e && e.message) || e) }), {
          status: 500,
          headers: { 'content-type': 'application/json' },
        })
      }
    }
    return new Response(JSON.stringify({ ok: true }), {
      headers: { 'content-type': 'application/json' },
    })
  },

  async queue(batch, env, ctx) {
    // Consumer path — the dispatcher invokes this via Fetcher.queue().
    // Every message increments the D1 counter; ackAll settles the
    // batch. A throw → the whole batch retries (at-least-once).
    await env.DB.exec(
      'CREATE TABLE IF NOT EXISTS queue_hits (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL)'
    )
    await env.DB.prepare(
      'INSERT INTO queue_hits (id, n) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET n = n + ?'
    )
      .bind(batch.messages.length, batch.messages.length)
      .run()
    batch.ackAll()
  },
}
