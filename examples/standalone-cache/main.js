// C1 standalone Cache API demo — caches.default put → match →
// delete → miss in one request so a single Host-header curl proves
// the whole lifecycle. Errors surface explicitly (`err:`) — a fake
// success can never pass as a real hit.
export default {
  async fetch(request) {
    const u = new URL(request.url);
    try {
      if (u.pathname === "/flow") {
        const key = "https://c.vrt.local/flow";
        await caches.default.put(
          key,
          new Response("F", { headers: { "Cache-Control": "max-age=60" } }),
        );
        const hit = await caches.default.match(key);
        const hitBody = hit ? await hit.text() : null;
        const del = await caches.default.delete(key);
        const after = await caches.default.match(key);
        return Response.json({
          hit: hitBody,
          del,
          after: after ? "hit" : "miss",
        });
      }
      if (u.pathname === "/named") {
        const c = await caches.open("demo-ns");
        const key = "https://c.vrt.local/named";
        await c.put(
          key,
          new Response("N", { headers: { "Cache-Control": "max-age=60" } }),
        );
        const hit = await c.match(key);
        const inDefault = await caches.default.match(key);
        return Response.json({
          hit: hit ? await hit.text() : null,
          defaultMiss: inDefault === undefined,
        });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return new Response(
        "err:" + String(e && e.message ? e.message : e).slice(0, 200),
        { status: 500 },
      );
    }
  },
};
