export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);
    if (pathname === "/ready") return Response.json({ ready: true });
    const result = await env.LIMITER.limit({ key: `${Date.now()}-${Math.random()}` });
    return Response.json({ success: result.success });
  },
};
