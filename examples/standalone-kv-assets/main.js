// /api/hits — KV read-modify-write counter. Everything else → ASSETS.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/api/hits") {
      const key = "hits";
      const current = Number((await env.CACHE.get(key)) ?? "0");
      const next = current + 1;
      await env.CACHE.put(key, String(next));
      return Response.json({ hits: next });
    }
    return env.ASSETS.fetch(request);
  },
};
