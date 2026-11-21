// R2 example (W5 e2e) — exercises the Cloudflare R2Bucket API served by the
// VRT R2 binding: put/get plus the multipart chain (create → parts →
// complete → get, abort, and signed-uploadId scope enforcement).
//
// NOTE: `env.BUCKET` is workerd's native `r2Bucket` binding. It is the
// upstream R2Bucket class: get() returns an R2ObjectBody (text/arrayBuffer/
// body/...), createMultipartUpload() and resumeMultipartUpload() return an
// R2MultipartUpload whose uploadPart()/complete()/abort() carry the key and
// uploadId. The facade translates workerd's native wire to the provider.
const PART = 5 * 1024 * 1024;

async function sha256hex(u8) {
  const buf = await crypto.subtle.digest("SHA-256", u8);
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key") || "obj";
    if (url.pathname === "/put") {
      const body = new Uint8Array(await request.arrayBuffer());
      await env.BUCKET.put(key, body);
      return Response.json({ ok: true, size: body.byteLength });
    }
    if (url.pathname === "/get") {
      const res = await env.BUCKET.get(key);
      if (!res) return new Response("missing", { status: 404 });
      const body = new Uint8Array(await res.arrayBuffer());
      return Response.json({ size: body.byteLength, sha256: await sha256hex(body) });
    }
    if (url.pathname === "/multipart") {
      // 3 parts: 5MiB + 5MiB + tail — the upstream non-final ≥5MiB rule.
      const parts = [
        new Uint8Array(PART).fill(0x61),
        new Uint8Array(PART).fill(0x62),
        new TextEncoder().encode("-tail"),
      ];
      const upload = await env.BUCKET.createMultipartUpload(key, {
        httpMetadata: { contentType: "application/octet-stream" },
        customMetadata: { origin: "e2e" },
      });
      const uploaded = [];
      for (let i = 0; i < parts.length; i++) {
        uploaded.push(await upload.uploadPart(i + 1, parts[i]));
      }
      const meta = await upload.complete(uploaded);
      const res = await env.BUCKET.get(key);
      const body = res ? new Uint8Array(await res.arrayBuffer()) : new Uint8Array(0);
      const expected = new Uint8Array(2 * PART + 5);
      expected.set(parts[0], 0);
      expected.set(parts[1], PART);
      expected.set(parts[2], 2 * PART);
      return Response.json({
        etag: meta.etag,
        size: body.byteLength,
        sha256: await sha256hex(body),
        expectedSize: expected.byteLength,
        expectedSha256: await sha256hex(expected),
        // complete() is a full R2Object: create-time metadata comes back
        // on the result itself, not only from a later get().
        completeCustomMetadata: meta.customMetadata?.origin ?? null,
        completeContentType: meta.httpMetadata?.contentType ?? null,
      });
    }
    if (url.pathname === "/abort") {
      const upload = await env.BUCKET.createMultipartUpload(key);
      await upload.uploadPart(1, new Uint8Array(8));
      await upload.abort();
      let rejected = false;
      try {
        // Resume by (key, uploadId) as a separate handle; the upload is
        // terminal, so completing it must fail.
        await env.BUCKET.resumeMultipartUpload(key, upload.uploadId).complete([]);
      } catch {
        rejected = true;
      }
      return Response.json({ completeAfterAbortRejected: rejected });
    }
    if (url.pathname === "/crosskey") {
      // uploadId minted for `key` must not operate on another key —
      // the HMAC scope check in the provider rejects it.
      const upload = await env.BUCKET.createMultipartUpload(key);
      let rejected = false;
      try {
        await env.BUCKET.resumeMultipartUpload("other-key", upload.uploadId).uploadPart(
          1,
          new Uint8Array(8),
        );
      } catch {
        rejected = true;
      }
      await upload.abort();
      return Response.json({ crossKeyRejected: rejected });
    }
    return new Response("not found", { status: 404 });
  },
};
