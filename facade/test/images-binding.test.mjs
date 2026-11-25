// Images binding relay (js.images.cloudflare.com → facade-images):
// passthrough fidelity, text-spec font injection via the egress
// policy, and cf-images-binding error shapes.
// Tests import the PUBLISHED dist bytes — never the .ts sources.
import test from "node:test";
import assert from "node:assert/strict";
import "./shims/register.mjs";
import { facadeModule } from "./helpers.mjs";

const { EgressProxy } = await import(facadeModule("egress/egress.js"));
const { relayImagesBinding } = await import(
  facadeModule("egress/images-binding.js")
);
const { POLICY_BINDING } = await import(facadeModule("egress/shared.js"));
const { parseEgressPolicy } = await import(facadeModule("egress/policy.js"));

const POLICY = {
  allowedPorts: [80, 443],
  allow: [{ host: "api.vibeploy.internal", port: 8443 }],
  denyNames: ["*.tenant.example", "blocked.example"],
  requestTimeoutMs: 30_000,
  tcpAllowedPorts: [],
  internalTcpAllow: [],
  tcpConnectTimeoutMs: 10_000,
  tcpIdleTimeoutMs: 60_000,
  tcpMaxConcurrent: 4,
};
const POLICY_JSON = JSON.stringify(POLICY);
const PARSED_POLICY = parseEgressPolicy(POLICY_JSON);

// OFL-licensed Kanit subset renamed "VRT Test Sans".
const FONT_BYTES = new Uint8Array(
  (await import("node:fs")).readFileSync(
    new URL("./fixtures/test-sans.ttf", import.meta.url),
  ),
);

const HOST = "https://js.images.cloudflare.com";

function post(pathname, fields) {
  const form = new FormData();
  for (const [k, v] of Object.entries(fields)) {
    form.append(k, v);
  }
  return new Request(`${HOST}${pathname}`, { method: "POST", body: form });
}

/** Recording svc fake — init.body kept as-is (FormData or bytes). */
function makeSvc(handler = () => new Response("ok", { status: 200 })) {
  const calls = [];
  const svc = {
    async fetch(input, init = {}) {
      const call = { url: String(input), init };
      calls.push(call);
      return handler(call);
    },
  };
  return { svc, calls };
}

/** Drain an init body (FormData → entries, else bytes). */
async function drainBody(init) {
  if (init.body instanceof FormData) {
    return { kind: "form", entries: [...init.body.entries()] };
  }
  if (init.body == null) return { kind: "empty" };
  return {
    kind: "bytes",
    bytes: new Uint8Array(await new Response(init.body).arrayBuffer()),
    contentType: new Headers(init.headers).get("content-type"),
  };
}

function stubFetch(handler) {
  const prev = globalThis.fetch;
  globalThis.fetch = handler;
  return () => {
    globalThis.fetch = prev;
  };
}

function pngStub() {
  const buf = new Uint8Array(33);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0x89504e47);
  dv.setUint32(16, 3);
  dv.setUint32(20, 2);
  return new File([buf], "src", { type: "image/png" });
}

test("relay: non-transform posts forward untouched", async () => {
  const { svc, calls } = makeSvc();
  const resp = await relayImagesBinding(
    post("/info", { image: pngStub() }),
    svc,
    PARSED_POLICY,
  );
  assert.equal(resp.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://vrt-images.internal/info");
  const body = await drainBody(calls[0].init);
  assert.equal(body.kind, "bytes");
  assert.match(body.contentType, /^multipart\/form-data; boundary=/);
});

test("relay: GET forwards with no body", async () => {
  const { svc, calls } = makeSvc();
  await relayImagesBinding(
    new Request(`${HOST}/bogus`, { method: "GET" }),
    svc,
    PARSED_POLICY,
  );
  assert.equal(calls[0].init.method, "GET");
  assert.equal(calls[0].init.body, undefined);
  assert.equal(calls[0].url, "http://vrt-images.internal/bogus");
});

test("relay: transform without text specs forwards raw bytes", async () => {
  const { svc, calls } = makeSvc();
  const req = post("/transform", {
    image: pngStub(),
    transforms: "[]",
  });
  const before = new Uint8Array(await req.clone().arrayBuffer());
  await relayImagesBinding(req, svc, POLICY);
  const body = await drainBody(calls[0].init);
  // Same bytes + boundary verbatim — no re-serialization.
  assert.deepEqual(body.bytes, before);
});

test("relay: text specs gain adjacent font files, order preserved", async () => {
  const { svc, calls } = makeSvc();
  const restore = stubFetch(async (url) => {
    assert.equal(String(url), "https://fonts.example/test.ttf");
    return new Response(FONT_BYTES);
  });
  try {
    const resp = await relayImagesBinding(
      post("/transform", {
        image: pngStub(),
        draw_image: pngStub(),
        draw_text: JSON.stringify({
          text: "HI",
          font: { url: "https://fonts.example/test.ttf" },
        }),
        text_input: JSON.stringify({
          text: "YO",
          font: { url: "https://fonts.example/test.ttf" },
        }),
        transforms: "[]",
      }),
      svc,
      PARSED_POLICY,
    );
    assert.equal(resp.status, 200);
  } finally {
    restore();
  }
  const body = await drainBody(calls[0].init);
  assert.equal(body.kind, "form");
  const names = body.entries.map(([n]) => n);
  assert.deepEqual(names, [
    "image",
    "draw_image",
    "draw_text",
    "draw_font",
    "text_input",
    "text_font",
    "transforms",
  ]);
  for (const field of ["draw_font", "text_font"]) {
    const file = body.entries.find(([n]) => n === field)?.[1];
    assert.ok(file instanceof File, `${field} must ride as a file`);
    assert.deepEqual(new Uint8Array(await file.arrayBuffer()), FONT_BYTES);
  }
});

test("relay: policy-denied font → err=9420, facade not called", async () => {
  const { svc, calls } = makeSvc();
  const restore = stubFetch(async () => {
    throw new Error("should not fetch a denied font url");
  });
  try {
    const resp = await relayImagesBinding(
      post("/transform", {
        text_input: JSON.stringify({
          text: "HI",
          font: { url: "https://blocked.example/f.ttf" },
        }),
        transforms: "[]",
      }),
      svc,
      PARSED_POLICY,
    );
    assert.equal(resp.status, 502);
    assert.equal(resp.headers.get("cf-images-binding"), "err=9420");
    assert.equal(calls.length, 0);
  } finally {
    restore();
  }
});

test("relay: unfetchable and oversized fonts → err=9420/9424", async () => {
  const spec = (url) =>
    post("/transform", {
      text_input: JSON.stringify({ text: "HI", font: { url } }),
      transforms: "[]",
    });
  {
    const { svc, calls } = makeSvc();
    const restore = stubFetch(async () => {
      throw new Error("connection refused");
    });
    try {
      const resp = await relayImagesBinding(
        spec("https://fonts.example/f.ttf"),
        svc,
        PARSED_POLICY,
      );
      assert.equal(resp.headers.get("cf-images-binding"), "err=9420");
      assert.equal(calls.length, 0);
    } finally {
      restore();
    }
  }
  {
    const { svc } = makeSvc();
    const restore = stubFetch(
      async () => new Response(new Uint8Array(64 * 1024 * 1024 + 1)),
    );
    try {
      const resp = await relayImagesBinding(
        spec("https://fonts.example/big.ttf"),
        svc,
        PARSED_POLICY,
      );
      assert.equal(resp.status, 413);
      assert.equal(resp.headers.get("cf-images-binding"), "err=9424");
    } finally {
      restore();
    }
  }
});

test("relay: specs without font.url forward for the facade's 9423", async () => {
  const { svc, calls } = makeSvc(
    () =>
      new Response("ERROR 9423\n", {
        status: 400,
        headers: { "cf-images-binding": "err=9423" },
      }),
  );
  const resp = await relayImagesBinding(
    post("/transform", {
      text_input: JSON.stringify({ text: "HI" }),
      draw_text: "not-json-at-all",
      transforms: "[]",
    }),
    svc,
    PARSED_POLICY,
  );
  assert.equal(resp.headers.get("cf-images-binding"), "err=9423");
  const body = await drainBody(calls[0].init);
  const names = body.entries.map(([n]) => n);
  assert.deepEqual(names, ["text_input", "draw_text", "transforms"]);
});

test("relay: marker bytes inside a file do not inject fonts", async () => {
  const { svc, calls } = makeSvc();
  // A file whose payload happens to contain the marker string — the
  // rebuild keeps every field and injects nothing.
  const tricky = new File(
    [new TextEncoder().encode('stuff name="draw_text" more')],
    "tricky",
  );
  await relayImagesBinding(
    post("/transform", { image: tricky, transforms: "[]" }),
    svc,
    PARSED_POLICY,
  );
  const body = await drainBody(calls[0].init);
  assert.deepEqual(
    body.entries.map(([n]) => n),
    ["image", "transforms"],
  );
});

test("relay: facade errors pass through with binding header", async () => {
  const { svc } = makeSvc(
    () =>
      new Response("ERROR 9420\n", {
        status: 415,
        headers: {
          "cf-images-binding": "err=9420",
          "x-vrt-internal": "drop-me",
        },
      }),
  );
  const resp = await relayImagesBinding(
    post("/info", { image: pngStub() }),
    svc,
    PARSED_POLICY,
  );
  assert.equal(resp.status, 415);
  assert.equal(resp.headers.get("cf-images-binding"), "err=9420");
  assert.equal(resp.headers.get("x-vrt-internal"), null);
});

test("proxy: js.images host intercepts before the egress policy", async () => {
  // Even a fail-closed (absent) policy must not block the binding
  // channel — the synthetic host is never a tenant destination.
  const { svc, calls } = makeSvc();
  const proxy = new EgressProxy({ props: {} }, { VRT_IMAGES: svc });
  const resp = await proxy.fetch(post("/info", { image: pngStub() }));
  assert.equal(resp.status, 200);
  assert.equal(calls.length, 1);
});

test("proxy: missing VRT_IMAGES surfaces err=9523 on the binding channel", async () => {
  const proxy = new EgressProxy({ props: {} }, {});
  const resp = await proxy.fetch(post("/info", { image: pngStub() }));
  assert.equal(resp.headers.get("cf-images-binding"), "err=9523");
});

test("relay: malformed multipart with a marker forwards raw", async () => {
  // Marker bytes present but the boundary is bogus — formData()
  // throws and the facade's own validation owns the rejection.
  const { svc, calls } = makeSvc(
    () =>
      new Response("ERROR 9423\n", {
        status: 400,
        headers: { "cf-images-binding": "err=9423" },
      }),
  );
  const body = new TextEncoder().encode(
    'name="draw_text" but no real boundary structure',
  );
  const resp = await relayImagesBinding(
    new Request(`${HOST}/transform`, {
      method: "POST",
      headers: { "content-type": "multipart/form-data; boundary=zzz" },
      body,
    }),
    svc,
    PARSED_POLICY,
  );
  assert.equal(resp.headers.get("cf-images-binding"), "err=9423");
  const got = await drainBody(calls[0].init);
  assert.deepEqual(got.bytes, body);
});

test("relay: non-http font urls deny at the policy layer", async () => {
  const { svc, calls } = makeSvc();
  const resp = await relayImagesBinding(
    post("/transform", {
      text_input: JSON.stringify({
        text: "HI",
        font: { url: "data:font/ttf;base64,AAAA" },
      }),
      transforms: "[]",
    }),
    svc,
    PARSED_POLICY,
  );
  assert.equal(resp.headers.get("cf-images-binding"), "err=9420");
  assert.equal(calls.length, 0);
});

test("relay: allow-entry font hosts inject like public ones", async () => {
  const { svc, calls } = makeSvc();
  const restore = stubFetch(async (url) => {
    assert.equal(String(url), "https://api.vibeploy.internal:8443/f.ttf");
    return new Response(FONT_BYTES);
  });
  try {
    await relayImagesBinding(
      post("/transform", {
        text_input: JSON.stringify({
          text: "HI",
          font: { url: "https://api.vibeploy.internal:8443/f.ttf" },
        }),
        transforms: "[]",
      }),
      svc,
      PARSED_POLICY,
    );
  } finally {
    restore();
  }
  const body = await drainBody(calls[0].init);
  const file = body.entries.find(([n]) => n === "text_font")?.[1];
  assert.ok(file instanceof File);
});

test("relay: multiple draw_text specs pair fonts in order", async () => {
  const { svc, calls } = makeSvc();
  const fonts = {
    "https://fonts.example/a.ttf": new Uint8Array([1, 2, 3]),
    "https://fonts.example/b.ttf": new Uint8Array([4, 5, 6]),
  };
  const restore = stubFetch(async (url) => new Response(fonts[String(url)]));
  // Same-named fields need a real FormData (object keys can't dup).
  const form = new FormData();
  form.append("image", pngStub());
  form.append(
    "draw_text",
    JSON.stringify({ text: "A", font: { url: "https://fonts.example/a.ttf" } }),
  );
  form.append("draw_image", pngStub());
  form.append(
    "draw_text",
    JSON.stringify({ text: "B", font: { url: "https://fonts.example/b.ttf" } }),
  );
  form.append("transforms", "[]");
  try {
    await relayImagesBinding(
      new Request(`${HOST}/transform`, { method: "POST", body: form }),
      svc,
      PARSED_POLICY,
    );
  } finally {
    restore();
  }
  const body = await drainBody(calls[0].init);
  const names = body.entries.map(([n]) => n);
  assert.deepEqual(names, [
    "image",
    "draw_text",
    "draw_font",
    "draw_image",
    "draw_text",
    "draw_font",
    "transforms",
  ]);
  const fontsOut = body.entries
    .filter(([n]) => n === "draw_font")
    .map(([, f]) => f);
  assert.deepEqual(
    new Uint8Array(await fontsOut[0].arrayBuffer()),
    fonts["https://fonts.example/a.ttf"],
  );
  assert.deepEqual(
    new Uint8Array(await fontsOut[1].arrayBuffer()),
    fonts["https://fonts.example/b.ttf"],
  );
});

test("relay: font redirect re-runs the policy per hop", async () => {
  const { svc, calls } = makeSvc();
  const restore = stubFetch(async (url) => {
    const u = String(url);
    if (u === "https://fonts.example/f.ttf") {
      return new Response(null, {
        status: 302,
        headers: { location: "http://blocked.example/f.ttf" },
      });
    }
    throw new Error("must not dial a denied redirect target: " + u);
  });
  try {
    const resp = await relayImagesBinding(
      post("/transform", {
        text_input: JSON.stringify({
          text: "HI",
          font: { url: "https://fonts.example/f.ttf" },
        }),
        transforms: "[]",
      }),
      svc,
      PARSED_POLICY,
    );
    assert.equal(resp.headers.get("cf-images-binding"), "err=9420");
    assert.equal(calls.length, 0);
  } finally {
    restore();
  }
});

test("relay: a dead facade surfaces err=9523, not a bare rejection", async () => {
  const svc = {
    async fetch() {
      throw new Error("service unreachable");
    },
  };
  const resp = await relayImagesBinding(
    post("/info", { image: pngStub() }),
    svc,
    PARSED_POLICY,
  );
  assert.equal(resp.status, 500);
  assert.equal(resp.headers.get("cf-images-binding"), "err=9523");
});

test("relay: /info streams its body instead of buffering", async () => {
  const { svc, calls } = makeSvc();
  await relayImagesBinding(
    post("/info", { image: pngStub() }),
    svc,
    PARSED_POLICY,
  );
  assert.ok(calls[0].init.body instanceof ReadableStream);
  const got = await drainBody(calls[0].init);
  assert.ok(got.bytes.length > 33);
});

test("proxy: relay e2e — injected font rasterizes through the facade", async () => {
  const { ImagesFacade } = await import(facadeModule("facade-images.js"));
  const eng = await import(facadeModule("images-engine.js"));
  const facade = new ImagesFacade({}, {});
  const svc = {
    fetch: (input, init) => facade.fetch(new Request(input, init)),
  };
  const proxy = new EgressProxy(
    { props: {} },
    { [POLICY_BINDING]: POLICY_JSON, VRT_IMAGES: svc },
  );
  const restore = stubFetch(async (url) => {
    assert.equal(String(url), "https://fonts.example/vrt-test-sans.ttf");
    return new Response(FONT_BYTES);
  });
  try {
    // Canvas is a real PNG; a draw_text spec pulls its font through
    // the relay and composites over the image.
    const src = await eng.encodeImage(
      (() => {
        const data = new Uint8ClampedArray(40 * 40 * 4).fill(200);
        return { data, width: 40, height: 40 };
      })(),
      "png",
      90,
    );
    const resp = await proxy.fetch(
      post("/transform", {
        image: new File([src.bytes], "src"),
        draw_text: JSON.stringify({
          text: "HI",
          font: { url: "https://fonts.example/vrt-test-sans.ttf" },
          size: 30,
          color: "#ff0000",
        }),
        transforms: JSON.stringify([
          { drawImageIndex: 1, targetImageIndex: 0, left: 0, top: 0 },
        ]),
        output_format: "image/png",
      }),
    );
    assert.equal(resp.status, 200);
    const out = new Uint8Array(await resp.arrayBuffer());
    assert.deepEqual([...out.slice(0, 4)], [0x89, 0x50, 0x4e, 0x47]);
  } finally {
    restore();
  }
});
