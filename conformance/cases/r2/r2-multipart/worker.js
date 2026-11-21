// R2 multipart conformance probe — create → parts (>=5MiB non-final,
// upstream rule) → complete → get, plus abort.
const PART = 5 * 1024 * 1024;

// Parts are passed as bytes: R2 rejects a ReadableStream of unknown length.
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

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    const key = url.searchParams.get("key") || "mp";
    try {
      if (url.pathname === "/multipart") {
        // 5MiB + 5MiB + 3B tail. The final part may be smaller; every
        // non-final part must meet the >=5MiB floor.
        const up = await env.BUCKET.createMultipartUpload(key, {
          httpMetadata: { contentType: "application/octet-stream" },
          customMetadata: { origin: "conformance" },
        });
        const parts = [];
        parts.push(await up.uploadPart(1, new Uint8Array(PART).fill(0x61)));
        parts.push(await up.uploadPart(2, new Uint8Array(PART).fill(0x62)));
        parts.push(await up.uploadPart(3, new Uint8Array([0x63, 0x63, 0x63])));
        const meta = await up.complete(parts);
        const res = await env.BUCKET.get(key, {});
        const body = await readAll(res.body);
        return j({
          uploadIdKey: up.key,
          partNumbers: parts.map((p) => p.partNumber),
          etagsOk: parts.every((p) => typeof p.etag === "string" && p.etag.length > 0),
          size: meta.size,
          bytes: body.byteLength,
          head: body[0] === 0x61 && body[PART] === 0x62 && body[body.byteLength - 1] === 0x63,
          // complete() is a full R2Object: metadata set at create time is
          // on the result itself, not only on a later get().
          completeCustomMetadata: meta.customMetadata ?? null,
          completeContentType: meta.httpMetadata?.contentType ?? null,
          // Cross-check that the stored object carries the same value.
          customMetadata: res.customMetadata ?? null,
        });
      }
      if (url.pathname === "/abort") {
        const up = await env.BUCKET.createMultipartUpload(key, {
          httpMetadata: {}, customMetadata: {},
        });
        await up.uploadPart(1, new Uint8Array(PART).fill(0x64));
        // Resume by id so resumeMultipartUpload is exercised too.
        await env.BUCKET.resumeMultipartUpload(key, up.uploadId).abort();
        const res = await env.BUCKET.get(key, {});
        return j({ getAfterAbort: res !== null });
      }
      if (url.pathname === "/bad-part-size") {
        // Upstream rule: a non-final part under 5MiB must be rejected.
        const up = await env.BUCKET.createMultipartUpload(key, {
          httpMetadata: {}, customMetadata: {},
        });
        let threw = false;
        let message = "";
        try {
          await up.uploadPart(1, new Uint8Array(16).fill(0x65));
          // completing with only an undersized part also has to fail —
          // final-part leniency applies to the LAST part only.
          await up.complete([
            { partNumber: 1, etag: "x" },
          ]);
        } catch (e) {
          threw = true;
          message = String(e?.message ?? e);
        }
        await up.abort().catch(() => {});
        return j({ threw, message });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e), name: e.name });
    }
  },
};
