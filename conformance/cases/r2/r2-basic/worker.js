// R2 conformance probe over env.BUCKET, written against the Cloudflare
// R2Bucket Workers API (head/get/put/delete/list), so the same worker
// runs unchanged on Cloudflare:
// - put/get/head return R2Object / R2ObjectBody, or null on a miss;
// - a put whose onlyIf precondition fails returns null;
// - a get whose onlyIf precondition fails returns an R2Object without a body;
// - bodies are passed as bytes or strings, because R2 rejects a
//   ReadableStream of unknown length.

const normMeta = (m) =>
  m === null ? null : {
    key: m.key,
    size: m.size,
    etagLen: typeof m.etag === "string" ? m.etag.length : 0,
    // httpEtag is the quoted form of the bare etag.
    httpEtagOk: m.httpEtag === `"${m.etag}"`,
    customMetadata: m.customMetadata ?? null,
    contentType: m.httpMetadata?.contentType ?? null,
    range: m.range ?? null,
    versionNonEmpty: typeof m.version === "string" && m.version.length > 0,
  };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    const key = url.searchParams.get("key") || "obj";
    try {
      if (url.pathname === "/put") {
        const body = new Uint8Array(await request.arrayBuffer());
        const meta = await env.BUCKET.put(key, body, {
          httpMetadata: { contentType: "text/plain" },
          customMetadata: { m: "v" },
        });
        return j({ meta: normMeta(meta) });
      }
      if (url.pathname === "/put-cond") {
        // mode=if-not-exists -> If-None-Match: *
        // mode=if-exists     -> If-Match: *
        const mode = url.searchParams.get("mode");
        const onlyIf = new Headers(
          mode === "if-exists" ? { "If-Match": "*" } : { "If-None-Match": "*" },
        );
        const meta = await env.BUCKET.put(key, "cond-body", { onlyIf });
        return j({ written: meta !== null });
      }
      if (url.pathname === "/get") {
        const res = await env.BUCKET.get(key);
        if (res === null) return j({ hit: false });
        return j({ hit: true, text: await res.text(), meta: normMeta(res) });
      }
      if (url.pathname === "/get-cond") {
        // GET keyed to the etag learned from a prior /head call.
        const etag = url.searchParams.get("etag") ?? "";
        const onlyIf = url.searchParams.get("mode") === "nomatch"
          ? { etagDoesNotMatch: etag }
          : { etagMatches: etag };
        const res = await env.BUCKET.get(key, { onlyIf });
        return j({ hit: res !== null && "body" in res });
      }
      if (url.pathname === "/head") {
        return j({ meta: normMeta(await env.BUCKET.head(key)) });
      }
      if (url.pathname === "/range") {
        const length = Number(url.searchParams.get("length") ?? 0);
        const range = { offset: Number(url.searchParams.get("offset") ?? 0) };
        if (length > 0) range.length = length;
        const res = await env.BUCKET.get(key, { range });
        if (res === null) return j({ hit: false });
        return j({
          hit: true,
          text: await res.text(),
          range: res.range ?? null,
          size: res.size,
        });
      }
      if (url.pathname === "/delete") {
        const { keys } = await request.json();
        await env.BUCKET.delete(keys);
        return j({ ok: true });
      }
      if (url.pathname === "/list") {
        const o = {
          prefix: url.searchParams.get("prefix") ?? "",
          limit: Number(url.searchParams.get("limit") ?? "1000"),
          include: ["httpMetadata", "customMetadata"],
        };
        const d = url.searchParams.get("delimiter");
        if (d) o.delimiter = d;
        const r = await env.BUCKET.list(o);
        return j({
          keys: r.objects.map((m) => normMeta(m)),
          names: r.objects.map((m) => m.key),
          truncated: r.truncated,
          delimitedPrefixes: r.delimitedPrefixes,
        });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e), name: e.name });
    }
  },
};
