export default {
  async fetch(request, env) {
    const url = new URL(request.url)
    if (url.pathname === '/api/hits') {
      // T34d: surface the real error — an uncaught throw becomes
      // workerd's opaque 500 page with no diagnostic anywhere.
      try {
        await env.DB.exec(
        'CREATE TABLE IF NOT EXISTS hits (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL)'
      )
      await env.DB.prepare(
        'INSERT INTO hits (id, n) VALUES (1, 1) ON CONFLICT(id) DO UPDATE SET n = n + 1'
      ).run()
        const row = await env.DB.prepare('SELECT n FROM hits WHERE id = 1').first()
        return new Response(JSON.stringify({ hits: row.n }), {
          headers: { 'content-type': 'application/json' },
        })
      } catch (e) {
        return new Response(JSON.stringify({ error: String(e && e.message || e) }), {
          status: 500,
          headers: { 'content-type': 'application/json' },
        })
      }
    }
    return new Response(JSON.stringify({ ok: true }), {
      headers: { 'content-type': 'application/json' },
    })
  },
}
