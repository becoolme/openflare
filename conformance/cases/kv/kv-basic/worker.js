// KV conformance probe — thin dispatch over env.KV so the runner's
// steps can call each documented KVNamespace method by path.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const p = url.pathname;
    const j = (v) => Response.json(v);
    try {
      if (p === "/put") {
        const { key, value, options } = await request.json();
        await env.KV.put(key, value, options);
        return j({ ok: true });
      }
      if (p === "/get") {
        const type = url.searchParams.get("type") ?? "text";
        const value = await env.KV.get(
          url.searchParams.get("key"),
          type === "text" ? undefined : { type },
        );
        if (type === "arrayBuffer" && value !== null)
          return j({ byteLength: value.byteLength, text: new TextDecoder().decode(value) });
        return j({ value });
      }
      if (p === "/getwm") {
        return j(await env.KV.getWithMetadata(url.searchParams.get("key")));
      }
      if (p === "/getmany") {
        const { keys } = await request.json();
        return j(Object.fromEntries(await env.KV.get(keys)));
      }
      if (p === "/getmany-wm") {
        const { keys } = await request.json();
        return j(Object.fromEntries(await env.KV.getWithMetadata(keys)));
      }
      if (p === "/delete") {
        const { key } = await request.json();
        await env.KV.delete(key);
        return j({ ok: true });
      }
      if (p === "/err") {
        // Routes a documented-invalid call and reports the thrown
        // client-side TypeError code instead of crashing.
        const { op, args } = await request.json();
        try {
          await env.KV[op](...(args ?? []));
          return j({ threw: false });
        } catch (e) {
          return j({ threw: true, name: e.name, message: String(e.message ?? e) });
        }
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e) });
    }
  },
};
