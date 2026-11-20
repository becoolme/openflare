// KV list() conformance probe. The worker normalizes each page's
// key objects so expectations stay deterministic across storage
// backends (names, metadata, expiration presence are what the API
// contract fixes; the raw cursor token stays opaque).
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    const norm = (k) => ({
      name: k.name,
      metadata: k.metadata ?? null,
      // Epoch value varies run to run — assert the contract (a future
      // unix-time number) instead of the literal value.
      hasValidExpiration:
        typeof k.expiration === "number" &&
        Number.isInteger(k.expiration) &&
        k.expiration > Date.now() / 1000,
    });
    try {
      if (url.pathname === "/reset") {
        // The namespace outlives redeploys; drop keys earlier runs
        // left behind so the pagination counts are exact.
        let cursor;
        for (let i = 0; i < 50; i++) {
          const r = await env.KV.list({ cursor });
          await Promise.all(r.keys.map((k) => env.KV.delete(k.name)));
          if (r.list_complete) return j({ ok: true });
          cursor = r.cursor;
        }
        return j({ error: "reset runaway" });
      }
      if (url.pathname === "/put") {
        const { key, value, options } = await request.json();
        await env.KV.put(key, value, options);
        return j({ ok: true });
      }
      if (url.pathname === "/list") {
        const o = {};
        for (const k of ["prefix", "cursor"]) {
          const v = url.searchParams.get(k);
          if (v !== null) o[k] = v;
        }
        const l = url.searchParams.get("limit");
        if (l !== null) o.limit = Number(l);
        const r = await env.KV.list(o);
        return j({
          keys: r.keys.map(norm),
          names: r.keys.map((k) => k.name),
          list_complete: r.list_complete,
          // workerd drops `cursor` entirely on a complete page and
          // hands back a non-empty string when truncated.
          cursorPresent: "cursor" in r,
          cursorNonEmpty: typeof r.cursor === "string" && r.cursor.length > 0,
        });
      }
      if (url.pathname === "/list-all") {
        const prefix = url.searchParams.get("prefix") ?? "";
        const limit = Number(url.searchParams.get("limit") ?? "2");
        const names = [];
        let pages = 0;
        let cursor;
        for (;;) {
          const r = await env.KV.list({ prefix, limit, cursor });
          names.push(...r.keys.map((k) => k.name));
          pages++;
          if (r.list_complete) break;
          cursor = r.cursor;
          if (pages > 20) return j({ error: "pagination runaway" });
        }
        return j({ names, pages });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e) });
    }
  },
};
