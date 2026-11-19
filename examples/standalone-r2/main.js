// R2 example (W5 e2e) — exercises the VRT R2 transport surface:
// put/get plus the multipart chain (create → parts → complete → get,
// abort, and signed-uploadId scope enforcement).
//
// NOTE: `env.BUCKET` is the vendored facade's R2Transport entrypoint —
// put/uploadPart take a ReadableStream body, get returns {meta, body},
// completeMultipartUpload takes {partNumber, etag} pairs. There is no
// upstream-style R2Object wrapper and no resumeMultipartUpload.
const PART = 5 * 1024 * 1024;

function streamOf(u8) {
  return new ReadableStream({
    start(c) {
      c.enqueue(u8);
      c.close();
    },
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
  for (const c of chunks) {
    out.set(c, off);
    off += c.byteLength;
  }
  return out;
}

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
      await env.BUCKET.put(key, streamOf(body));
      return Response.json({ ok: true, size: body.byteLength });
    }
    if (url.pathname === "/get") {
      const res = await env.BUCKET.get(key);
      if (!res || res.body === undefined) return new Response("missing", { status: 404 });
      const body = await readAll(res.body);
      return Response.json({ size: body.byteLength, sha256: await sha256hex(body) });
    }
    if (url.pathname === "/multipart") {
      // 3 parts: 5MiB + 5MiB + tail — the upstream non-final ≥5MiB rule.
      const parts = [
        new Uint8Array(PART).fill(0x61),
        new Uint8Array(PART).fill(0x62),
        new TextEncoder().encode("-tail"),
      ];
      const { uploadId } = await env.BUCKET.createMultipartUpload(key, {
        customMetadata: { origin: "e2e" },
      });
      const uploaded = [];
      for (let i = 0; i < parts.length; i++) {
        uploaded.push(await env.BUCKET.uploadPart(key, uploadId, i + 1, streamOf(parts[i])));
      }
      const meta = await env.BUCKET.completeMultipartUpload(key, uploadId, uploaded);
      const res = await env.BUCKET.get(key);
      const body = res && res.body !== undefined ? await readAll(res.body) : new Uint8Array(0);
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
      });
    }
    if (url.pathname === "/abort") {
      const { uploadId } = await env.BUCKET.createMultipartUpload(key);
      await env.BUCKET.uploadPart(key, uploadId, 1, streamOf(new Uint8Array(8)));
      await env.BUCKET.abortMultipartUpload(key, uploadId);
      let rejected = false;
      try {
        await env.BUCKET.completeMultipartUpload(key, uploadId, []);
      } catch {
        rejected = true;
      }
      return Response.json({ completeAfterAbortRejected: rejected });
    }
    if (url.pathname === "/crosskey") {
      // uploadId minted for `key` must not operate on another key —
      // the HMAC scope check in the provider rejects it.
      const { uploadId } = await env.BUCKET.createMultipartUpload(key);
      let rejected = false;
      try {
        await env.BUCKET.uploadPart("other-key", uploadId, 1, streamOf(new Uint8Array(8)));
      } catch {
        rejected = true;
      }
      await env.BUCKET.abortMultipartUpload(key, uploadId);
      return Response.json({ crossKeyRejected: rejected });
    }
    return new Response("not found", { status: 404 });
  },
};
