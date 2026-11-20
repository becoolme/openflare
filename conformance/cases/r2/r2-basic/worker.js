// R2 conformance probe over env.BUCKET — the vendored R2Transport
// WorkerEntrypoint surface (head/get/put/delete/list + multipart).
// NOTE: this is OpenFlare's binding layer, not the stock `R2Bucket`
// JS wrapper: put takes a ReadableStream, get returns {meta, body},
// and conditions use R2Condition {etagMatches, etagDoesNotMatch}.
// Semantics mirrored from the upstream binding contract are asserted
// here (miss → null, precondition-fail → null, list result shape).

function streamOf(u8) {
  return new ReadableStream({
    start(c) { c.enqueue(u8); c.close(); },
  });
}

async function readAll(stream) {
  const chunks = [];
  const reader = stream.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  const out = new Uint8Array(chunks.reduce((n, c) => n + c.byteLength, 0));
  let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.byteLength; }
  return out;
}

const enc = new TextEncoder();
const dec = new TextDecoder();

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    const key = url.searchParams.get("key") || "obj";
    const normMeta = (m) =>
      m === null ? null : {
        key: m.key,
        size: m.size,
        etagLen: typeof m.etag === "string" ? m.etag.length : 0,
        // httpEtag is the quoted form of the bare etag (r2_memory.rs
        // asserts the same `\"<etag>\"` shape provider-side).
        httpEtagOk: m.httpEtag === `"${m.etag}"`,
        customMetadata: m.customMetadata ?? null,
        contentType: m.httpMetadata?.contentType ?? null,
        range: m.range ?? null,
        versionNonEmpty: typeof m.version === "string" && m.version.length > 0,
      };
    try {
      if (url.pathname === "/put") {
        const body = new Uint8Array(await request.arrayBuffer());
        const meta = await env.BUCKET.put(key, streamOf(body), {
          httpMetadata: { contentType: "text/plain" },
          customMetadata: { m: "v" },
        });
        return j({ meta: normMeta(meta) });
      }
      if (url.pathname === "/put-cond") {
        // mode=if-not-exists → etagDoesNotMatch wildcard
        // mode=if-exists     → etagMatches wildcard
        const mode = url.searchParams.get("mode");
        const onlyIf =
          mode === "if-exists"
            ? { etagMatches: [{ kind: "wildcard" }], etagDoesNotMatch: [] }
            : { etagMatches: [], etagDoesNotMatch: [{ kind: "wildcard" }] };
        const meta = await env.BUCKET.put(
          key,
          streamOf(enc.encode("cond-body")),
          { httpMetadata: {}, customMetadata: {}, onlyIf },
        );
        return j({ written: meta !== null });
      }
      if (url.pathname === "/get") {
        const res = await env.BUCKET.get(key, {});
        if (res === null) return j({ hit: false });
        const body = await readAll(res.body);
        return j({ hit: true, text: dec.decode(body), meta: normMeta(res.meta) });
      }
      if (url.pathname === "/get-cond") {
        // GET keyed to the etag learned from a prior /head call.
        const etag = url.searchParams.get("etag") ?? "";
        const matches = url.searchParams.get("mode") === "nomatch";
        const onlyIf = matches
          ? { etagMatches: [], etagDoesNotMatch: [{ kind: "strong", value: etag }] }
          : { etagMatches: [{ kind: "strong", value: etag }], etagDoesNotMatch: [] };
        const res = await env.BUCKET.get(key, { onlyIf });
        return j({ hit: res !== null });
      }
      if (url.pathname === "/head") {
        return j({ meta: normMeta(await env.BUCKET.head(key)) });
      }
      if (url.pathname === "/range") {
        const res = await env.BUCKET.get(key, {
          range: {
            offset: Number(url.searchParams.get("offset") ?? 0),
            length: Number(url.searchParams.get("length") ?? 0) || undefined,
          },
        });
        if (res === null) return j({ hit: false });
        return j({
          hit: true,
          text: dec.decode(await readAll(res.body)),
          range: res.meta.range ?? null,
          size: res.meta.size,
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
