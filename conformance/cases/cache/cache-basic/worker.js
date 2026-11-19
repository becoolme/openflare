// Cache API probe — one request covers the lifecycle so the runner
// stays declarative (same pattern as examples/standalone-cache).
export default {
  async fetch(request) {
    const u = new URL(request.url);
    try {
      if (u.pathname === "/flow") {
        const key = "https://conf.vrt.local/flow";
        const miss = await caches.default.match(key);
        await caches.default.put(
          key,
          new Response("F", { headers: { "Cache-Control": "max-age=60" } }),
        );
        const hit = await caches.default.match(key);
        const hitBody = hit ? await hit.text() : null;
        const del = await caches.default.delete(key);
        const after = await caches.default.match(key);
        return Response.json({ miss: miss === undefined, hit: hitBody, del, after: after === undefined });
      }
      if (u.pathname === "/named") {
        const c = await caches.open("conf-ns");
        const key = "https://conf.vrt.local/named";
        await c.put(
          key,
          new Response("N", { headers: { "Cache-Control": "max-age=60" } }),
        );
        const hit = await c.match(key);
        const inDefault = await caches.default.match(key);
        return Response.json({ hit: hit ? await hit.text() : null, defaultMiss: inDefault === undefined });
      }
      if (u.pathname === "/nostore") {
        // A response the requester marked no-store must not be
        // stored; the follow-up match misses.
        const key = "https://conf.vrt.local/nostore";
        await caches.default.put(
          new Request(key, { headers: { "cache-control": "no-store" } }),
          new Response("S", { headers: { "Cache-Control": "max-age=60" } }),
        );
        const after = await caches.default.match(key);
        return Response.json({ after: after === undefined });
      }
      if (u.pathname === "/response-headers") {
        // A cached response must round-trip its headers and body.
        const key = "https://conf.vrt.local/hdr";
        await caches.default.put(
          key,
          new Response("H", { headers: { "Cache-Control": "max-age=60", "x-conf": "yes" } }),
        );
        const hit = await caches.default.match(key);
        return Response.json({ body: hit ? await hit.text() : null, xConf: hit?.headers.get("x-conf") ?? null });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return new Response("err:" + String(e?.message ?? e).slice(0, 200), { status: 500 });
    }
  },
};
