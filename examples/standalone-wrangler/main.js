export default {
  async fetch(request, env) {
    const url = new URL(request.url)
    if (url.pathname === '/api/hits') {
      const n = (Number(await env.CACHE.get('hits')) || 0) + 1
      await env.CACHE.put('hits', String(n))
      return new Response(JSON.stringify({ hits: n }), {
        headers: { 'content-type': 'application/json' },
      })
    }
    return new Response(
      JSON.stringify({ greeting: env.GREETING, limit: env.LIMIT }),
      { headers: { 'content-type': 'application/json' } }
    )
  },
}
