// `cf.image` egress handling — the option parser, multipart body, and
// the EgressProxy fetch path's transform branch (stubbed upstream +
// stubbed VRT_IMAGES service binding).
import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule } from "./helpers.mjs";

const { cfImagePlan, cfImageBody, SRC_MAX } = await import(
  facadeModule("egress/cf-image.js")
);
const { EgressProxy } = await import(facadeModule("egress/egress.js"));
const { ImagesFacade } = await import(facadeModule("facade-images.js"));
const eng = await import(facadeModule("images-engine.js"));
const { POLICY_BINDING } = await import(facadeModule("egress/shared.js"));

const PNG_URL = "https://cdn.example.com/cat.png";

// Plain-object request — the same convention as egress.test.mjs: the
// entrypoint only reads url/headers/cf, and a Request cannot carry cf.
function pngReq(cfImage) {
  return {
    url: PNG_URL,
    headers: new Headers(),
    cf: cfImage === undefined ? undefined : { image: cfImage },
  };
}

// Full-shape policy (mirrors egress.test.mjs) — parse failure fails
// closed to deny-all, so a minimal document would deny the upstream.
const POLICY = {
  allowedPorts: [80, 443],
  allow: [],
  denyNames: [],
  requestTimeoutMs: 30_000,
  tcpAllowedPorts: [],
  internalTcpAllow: [],
  tcpConnectTimeoutMs: 10_000,
  tcpIdleTimeoutMs: 60_000,
  tcpMaxConcurrent: 4,
};

function egressEnv(imagesFetcher) {
  return {
    [POLICY_BINDING]: JSON.stringify(POLICY),
    ...(imagesFetcher ? { VRT_IMAGES: imagesFetcher } : {}),
  };
}

/** A 4x4 PNG produced by the vendored engine. */
async function sourcePng() {
  const { bytes } = await eng.encodeImage(
    { data: new Uint8ClampedArray(4 * 4 * 4).fill(200), width: 4, height: 4 },
    "png",
    90,
  );
  return bytes;
}

/** Route the transform POST at a real facade-images handler. */
const imagesFacade = new ImagesFacade({}, {});
const realImagesFetcher = {
  fetch: (input, init) => imagesFacade.fetch(new Request(input, init)),
};

function withStubbedUpstream(bytes, fn) {
  const prev = globalThis.fetch;
  globalThis.fetch = async () => new Response(bytes, { status: 200 });
  return fn().finally(() => {
    globalThis.fetch = prev;
  });
}

/** Per-URL upstream stub for `draw` tests — overlay URLs fan out
 * through the same globalThis.fetch as the source. A missing map key
 * rejects (unreachable overlay). */
function withUrlMap(map, fn) {
  const prev = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = typeof input === "string" ? input : input.url;
    if (map[url] === undefined) throw new TypeError("fetch failed");
    const [bytes, status] = map[url];
    return new Response(bytes, { status });
  };
  return fn().finally(() => {
    globalThis.fetch = prev;
  });
}

function makeProxy(env) {
  return new EgressProxy({ props: {} }, env);
}

test("cfImagePlan: absent cf / absent image → undefined", () => {
  assert.equal(cfImagePlan(pngReq(undefined)), undefined);
  assert.equal(cfImagePlan(pngReq(null)), undefined);
  assert.equal(cfImagePlan({ url: PNG_URL, headers: new Headers() }), undefined);
});

test("cfImagePlan: option vocabulary → transforms JSON", () => {
  const plan = cfImagePlan(
    pngReq({ width: 100, fit: "cover", format: "webp", quality: 80, dpr: 2 }),
  );
  assert.equal(plan.outputFormat, "image/webp");
  assert.equal(plan.outputQuality, 80);
  const op = JSON.parse(plan.transforms)[0];
  assert.equal(op.width, 200); // dpr applied
  assert.equal(op.fit, "cover");
});

test("cfImagePlan: unknown option + bad values throw TypeError", () => {
  for (const image of [
    { bogus: 1 },
    { width: 0 },
    { width: "wide" },
    { rotate: 45 },
    { draw: {} },
    { "origin-auth": "x" },
    { quality: 0 },
    { quality: "ultra" },
    { dpr: 3 }, // upstream caps dpr at 2
    { flip: "diagonal" },
    { compression: "best" },
    { fit: { upscale: "interpolate" } }, // mode required
    { sharpen: { mode: "gaussian", amount: 1 } }, // sigma required
    { sharpen: { mode: "box", amount: 1, sigma: 1 } },
    { sharpen: { amount: 11 } },
    { sharpen: "sharp" },
    { trim: { mode: "color", top: 5 } }, // mode mixing rejected
    { trim: { mode: "color", tolerance: 300 } },
    { trim: { mode: "area", width: 4, right: 5 } }, // rect + side keys
    { trim: { mode: "area", left: "200q" } },
    { trim: { mode: "bogus" } },
  ]) {
    assert.throws(
      () => cfImagePlan(pngReq(image)),
      TypeError,
      JSON.stringify(image),
    );
  }
});

test("cfImagePlan: structured sharpen/trim/fit/flip reach the wire", () => {
  const plan = cfImagePlan(
    pngReq({
      sharpen: { mode: "gaussian", amount: 1, sigma: 1.5, threshold: 3 },
      trim: { mode: "color", tolerance: 10, keep: 4 },
      fit: { mode: "contain", upscale: "interpolate" },
      flip: "hv",
      quality: "high",
    }),
  );
  const op = JSON.parse(plan.transforms)[0];
  assert.deepEqual(op.sharpen, { mode: "gaussian", amount: 1, sigma: 1.5, threshold: 3 });
  assert.deepEqual(op.trim, { mode: "color", tolerance: 10, keep: 4 });
  assert.deepEqual(op.fit, { mode: "contain", upscale: "interpolate" });
  assert.equal(op.flip, "hv");
  assert.equal(plan.outputQuality, 90); // perceptual tier anchor
});

test("cfImagePlan: trim lengths and border/padding scale with dpr", () => {
  const plan = cfImagePlan(
    pngReq({
      dpr: 2,
      trim: { mode: "area", top: 10, right: "20p", bottom: 0.1, left: 4 },
      border: { width: 5, color: "#ff0000" },
    }),
  );
  const op = JSON.parse(plan.transforms)[0];
  // px counts ×dpr; "Np" percents and 0<v<1 fractions pass through for
  // the facade to resolve against the real axis.
  assert.deepEqual(op.trim, { mode: "area", top: 20, right: "20p", bottom: 0.1, left: 8 });
  assert.deepEqual(op.border, { width: 10, color: "#ff0000" });

  // padding rides border with the background color (or transparent).
  const pad = cfImagePlan(pngReq({ padding: 6 }));
  const pop = JSON.parse(pad.transforms)[0];
  assert.deepEqual(pop.border, {
    width: 6,
    color: "rgba(0,0,0,0)",
  });
  const padBg = cfImagePlan(pngReq({ padding: 6, background: "rgb(1,2,3)" }));
  assert.equal(JSON.parse(padBg.transforms)[0].border.color, "rgb(1,2,3)");
});

test("cfImagePlan: compression=fast forces jpeg over format", () => {
  const plan = cfImagePlan(pngReq({ format: "webp", compression: "fast" }));
  assert.equal(plan.outputFormat, "image/jpeg");
  const plain = cfImagePlan(pngReq({ compression: "fast" }));
  assert.equal(plain.outputFormat, "image/jpeg");
});

test("cfImagePlan: trim string + color-mode keep scale with dpr", () => {
  const plan = cfImagePlan(pngReq({ trim: "10,20,10,20", dpr: 2 }));
  assert.deepEqual(JSON.parse(plan.transforms)[0].trim, {
    top: 20, right: 40, bottom: 20, left: 40,
  });
  const keep = cfImagePlan(pngReq({ trim: { mode: "color", keep: 3 }, dpr: 2 }));
  assert.equal(JSON.parse(keep.transforms)[0].trim.keep, 6);
});

test("cf.image end-to-end: upstream PNG → webp through real facade", async () => {
  const png = await sourcePng();
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withStubbedUpstream(png, async () => {
    const resp = await proxy.fetch(pngReq({ width: 2, format: "webp" }));
    assert.equal(resp.status, 200);
    const out = new Uint8Array(await resp.arrayBuffer());
    // RIFF header.
    assert.deepEqual([...out.subarray(0, 4)], [0x52, 0x49, 0x46, 0x46]);
    assert.equal(resp.headers.get("cf-resized"), null);
  });
});

test("cf.image absent → upstream bytes pass through untouched", async () => {
  const png = await sourcePng();
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withStubbedUpstream(png, async () => {
    const resp = await proxy.fetch(pngReq(undefined));
    assert.equal(resp.status, 200);
    const out = new Uint8Array(await resp.arrayBuffer());
    // Source PNG magic passes through.
    assert.deepEqual([...out.subarray(1, 4)], [0x50, 0x4e, 0x47]);
  });
});

test("cf.image transform error → cf-resized err header + ERROR body", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  // Not an image — facade decoders must type 9420.
  await withStubbedUpstream(new Uint8Array([1, 2, 3, 4]), async () => {
    const resp = await proxy.fetch(pngReq({ width: 2, format: "webp" }));
    assert.equal(resp.headers.get("cf-resized"), "err=9420");
    const body = await resp.text();
    assert.match(body, /^ERROR 9420/);
  });
});

test("cf.image onerror=redirect → 302 to original url", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withStubbedUpstream(new Uint8Array([1, 2, 3, 4]), async () => {
    const resp = await proxy.fetch(pngReq({ width: 2, onerror: "redirect" }));
    assert.equal(resp.status, 302);
    assert.equal(resp.headers.get("location"), PNG_URL);
  });
});

test("cf.image format=json → transformed metadata", async () => {
  const png = await sourcePng();
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withStubbedUpstream(png, async () => {
    const resp = await proxy.fetch(
      pngReq({ width: 2, height: 2, fit: "squeeze", format: "json" }),
    );
    assert.equal(resp.status, 200);
    const meta = await resp.json();
    assert.equal(meta.width, 2);
    assert.equal(meta.height, 2);
    assert.match(meta.format, /^image\//);
  });
});

test("cf.image end-to-end: structured options through real facade", async () => {
  // 16x16 red-bordered source with a blue center and a step edge —
  // trim + gaussian sharpen + flip all execute for real.
  const px = (x, y) =>
    x < 4 || x >= 12 || y < 4 || y >= 12 ? [255, 0, 0, 255] : [0, 0, x < 10 ? 60 : 200, 255];
  const data = new Uint8ClampedArray(16 * 16 * 4);
  for (let y = 0; y < 16; y++) {
    for (let x = 0; x < 16; x++) {
      const i = (y * 16 + x) * 4;
      const [r, g, b, a] = px(x, y);
      data[i] = r; data[i + 1] = g; data[i + 2] = b; data[i + 3] = a;
    }
  }
  const { bytes: png } = await eng.encodeImage({ data, width: 16, height: 16 }, "png", 100);
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withStubbedUpstream(png, async () => {
    const resp = await proxy.fetch(
      pngReq({
        trim: { mode: "color" },
        sharpen: { mode: "gaussian", amount: 2, sigma: 1 },
        flip: "h",
        format: "png",
      }),
    );
    assert.equal(resp.status, 200);
    const out = await eng.decodeImage(new Uint8Array(await resp.arrayBuffer()), "png");
    assert.equal(out.width, 8); // border trimmed to the 8x8 center
    assert.equal(out.height, 8);
    // After h-flip the bright half sits left (the step is in blue).
    assert.ok(out.data[2] > 150, `flipped blue channel ${out.data[2]}`);
  });
});

test("cf.image missing VRT_IMAGES binding → typed 9490 (never silent)", async () => {
  const proxy = makeProxy(egressEnv(undefined));
  await withStubbedUpstream(await sourcePng(), async () => {
    const resp = await proxy.fetch(pngReq({ width: 2, format: "webp" }));
    assert.equal(resp.headers.get("cf-resized"), "err=9490");
  });
});

test("cfImageBody: multipart fields match the binding wire", async () => {
  const plan = cfImagePlan(pngReq({ width: 8, format: "png", quality: 70 }));
  const fd = cfImageBody(new Uint8Array([0x89, 0x50]), plan);
  assert.ok(fd.get("image") instanceof File);
  assert.equal(fd.get("transforms"), plan.transforms);
  assert.equal(fd.get("output_format"), "image/png");
  assert.equal(fd.get("output_quality"), "70");
  assert.equal(fd.get("anim"), null);
});

test("SRC_MAX mirrors the binding's 64MiB input cap", () => {
  assert.equal(SRC_MAX, 64 * 1024 * 1024);
});

// SVG sources are svg-hush sanitized + passed through upstream — the
// transform POST never happens and every plan option is ignored.
const SVG_SRC =
  '<svg xmlns="http://www.w3.org/2000/svg">' +
  '<script>alert(1)</script><rect width="4"/></svg>';
const enc = new TextEncoder();

test("cf.image SVG source → sanitized image/svg+xml, VRT_IMAGES untouched", async () => {
  let called = 0;
  const countingFetcher = {
    fetch: (input, init) => {
      called++;
      return realImagesFetcher.fetch(input, init);
    },
  };
  const proxy = makeProxy(egressEnv(countingFetcher));
  await withStubbedUpstream(enc.encode(SVG_SRC), async () => {
    const resp = await proxy.fetch(pngReq({ width: 2, format: "webp" }));
    assert.equal(resp.status, 200);
    assert.equal(resp.headers.get("content-type"), "image/svg+xml");
    const body = await resp.text();
    assert.ok(body.includes("<rect"));
    assert.ok(!body.includes("script"));
  });
  assert.equal(called, 0);
});

test("cf.image SVG source + format:\"json\" → still image/svg+xml", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withStubbedUpstream(enc.encode(SVG_SRC), async () => {
    const resp = await proxy.fetch(pngReq({ format: "json" }));
    assert.equal(resp.headers.get("content-type"), "image/svg+xml");
  });
});

test("cf.image malformed SVG → ERROR 9420 + cf-resized", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withStubbedUpstream(
    enc.encode('<svg xmlns="http://www.w3.org/2000/svg"><rect'),
    async () => {
      const resp = await proxy.fetch(pngReq({ width: 2 }));
      assert.equal(resp.headers.get("cf-resized"), "err=9420");
      assert.match(await resp.text(), /ERROR 9420/);
    },
  );
});

test("cf.image malformed SVG + onerror:\"redirect\" → 302 like transform errors", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withStubbedUpstream(
    enc.encode('<svg xmlns="http://www.w3.org/2000/svg"><rect'),
    async () => {
      const resp = await proxy.fetch(pngReq({ onerror: "redirect" }));
      assert.equal(resp.status, 302);
      assert.equal(resp.headers.get("location"), PNG_URL);
    },
  );
});

// ---- draw overlays ----------------------------------------------------

const OVER_URL = "https://cdn.example.com/logo.png";

test("cfImagePlan: draw entry → url list + paired ops", () => {
  const plan = cfImagePlan(
    pngReq({
      width: 100,
      draw: [
        { url: OVER_URL, bottom: 5, right: 5, fit: "contain", width: 50, opacity: 0.8 },
        { url: "https://cdn.example.com/wm.png", repeat: "x", top: 0 },
      ],
    }),
  );
  assert.deepEqual(plan.draws, [
    { kind: "image", url: OVER_URL },
    { kind: "image", url: "https://cdn.example.com/wm.png" },
  ]);
  const ops = JSON.parse(plan.transforms);
  assert.equal(ops.length, 4); // main + over + draw, over + draw
  assert.deepEqual(ops[0], { width: 100 });
  // Overlay transform op: imageIndex + percentBasis marker.
  assert.deepEqual(ops[1], {
    imageIndex: 1, percentBasis: 0, fit: "contain", width: 50,
  });
  assert.deepEqual(ops[2], {
    drawImageIndex: 1, targetImageIndex: 0, bottom: 5, right: 5, opacity: 0.8,
  });
  // No transform keys → no overlay op.
  assert.deepEqual(ops[3], {
    drawImageIndex: 2, targetImageIndex: 0, repeat: "x", top: 0,
  });
});

test("cfImagePlan: draw percent dims pass through for base resolution", () => {
  const plan = cfImagePlan(
    pngReq({ draw: [{ url: OVER_URL, width: "25p", height: "50p" }] }),
  );
  const ops = JSON.parse(plan.transforms);
  assert.equal(ops[1].width, "25p");
  assert.equal(ops[1].height, "50p");
  assert.equal(ops[1].percentBasis, 0);
});

test("cfImagePlan: draw entries scale px values with dpr", () => {
  const plan = cfImagePlan(
    pngReq({ dpr: 2, draw: [{ url: OVER_URL, right: 5, width: 40 }] }),
  );
  const ops = JSON.parse(plan.transforms);
  assert.equal(ops[1].width, 80);
  assert.equal(ops[2].right, 10);
});

test("cfImagePlan: invalid draw entries throw TypeError", () => {
  for (const draw of [
    "not-an-array",
    [{ url: OVER_URL, left: 1, right: 2 }], // axis conflict
    [{ url: OVER_URL, top: 0, bottom: 0 }],
    [{ text: "HELLO" }], // font required for text
    [{ text: "HELLO", font: { url: OVER_URL }, url: OVER_URL }], // url is image-only
    [{ text: "HELLO", font: { url: OVER_URL }, width: 50 }], // transform on text
    [{ text: "HELLO", font: { url: OVER_URL }, color: "var(--x)" }],
    [{ text: "HELLO", font: { url: OVER_URL }, size: 0 }],
    [{ text: "HELLO", font: { url: "file:///etc/passwd" } }],
    [{ text: "HELLO", font: "x" }],
    [{ text: "" }],
    [{ url: OVER_URL, font: { url: "x" } }], // text-only key on image entry
    [{ url: OVER_URL, size: 20 }],
    [{ bottom: 5 }], // url required
    [{ url: "notaurl" }],
    [{ url: "file:///etc/passwd" }],
    [{ url: OVER_URL, opacity: 1.5 }],
    [{ url: OVER_URL, repeat: "xy" }], // url-iface accepts true|x|y
    [{ url: OVER_URL, composite: "screen" }],
    [{ url: OVER_URL, bogus: 1 }],
    [{ url: OVER_URL, width: "0p" }],
    [{ url: OVER_URL, width: "nope" }],
    [42],
  ]) {
    assert.throws(
      () => cfImagePlan(pngReq({ draw })),
      TypeError,
      JSON.stringify(draw),
    );
  }
});

test("cfImagePlan: draw entry cap mirrors MAX_DRAW_IMAGES", () => {
  const draw = Array.from({ length: 17 }, () => ({ url: OVER_URL }));
  assert.throws(() => cfImagePlan(pngReq({ draw })), TypeError);
  assert.doesNotThrow(() =>
    cfImagePlan(pngReq({ draw: draw.slice(0, 16) })),
  );
});

test("cf.image draw e2e: overlay composites bottom-right at 50%", async () => {
  const base = await sourcePng();
  // 2x2 red overlay on the 4x4 base.
  const { bytes: over } = await eng.encodeImage(
    { data: new Uint8ClampedArray(2 * 2 * 4).fill(0).map((_, i) => [255, 0, 0, 255][i % 4]), width: 2, height: 2 },
    "png",
    100,
  );
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withUrlMap(
    { [PNG_URL]: [base, 200], [OVER_URL]: [over, 200] },
    async () => {
      const resp = await proxy.fetch(
        pngReq({ format: "png", draw: [{ url: OVER_URL, bottom: 0, right: 0, opacity: 0.5 }] }),
      );
      assert.equal(resp.status, 200);
      const out = await eng.decodeImage(
        new Uint8Array(await resp.arrayBuffer()),
        "png",
      );
      const px = (x, y) => {
        const i = (y * 4 + x) * 4;
        return [out.data[i], out.data[i + 1]];
      };
      // bottom-right pixel blended 50% red over the gray source;
      // top-left untouched.
      const [r, g] = px(3, 3);
      assert.ok(r > 150 && r > g, `corner blend ${px(3, 3)}`);
      assert.equal(px(0, 0)[0], px(0, 0)[1]);
    },
  );
});

test("cf.image draw: unfetchable overlay → ERROR 9420 + cf-resized", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withUrlMap({ [PNG_URL]: [await sourcePng(), 200] }, async () => {
    const resp = await proxy.fetch(
      pngReq({ draw: [{ url: OVER_URL }] }),
    );
    assert.equal(resp.headers.get("cf-resized"), "err=9420");
    assert.match(await resp.text(), /^ERROR 9420/);
  });
});

test("cf.image draw: unfetchable overlay + onerror:redirect → 302", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withUrlMap({ [PNG_URL]: [await sourcePng(), 200] }, async () => {
    const resp = await proxy.fetch(
      pngReq({ onerror: "redirect", draw: [{ url: OVER_URL }] }),
    );
    assert.equal(resp.status, 302);
    assert.equal(resp.headers.get("location"), PNG_URL);
  });
});

test("cf.image draw: policy-denied overlay → ERROR 9420", async () => {
  const proxy = makeProxy(
    egressEnv(realImagesFetcher),
  );
  await withUrlMap({ [PNG_URL]: [await sourcePng(), 200] }, async () => {
    const resp = await proxy.fetch(
      pngReq({ draw: [{ url: "http://169.254.169.254/meta.png" }] }),
    );
    assert.equal(resp.headers.get("cf-resized"), "err=9420");
  });
});

test("cf.image draw: overlay redirect re-runs policy per hop", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  const prev = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = typeof input === "string" ? input : input.url;
    if (url === PNG_URL) return new Response(await sourcePng(), { status: 200 });
    if (url === OVER_URL) {
      // Allowed host 302s to a private literal — must not be dialed.
      return new Response(null, {
        status: 302,
        headers: { location: "http://169.254.169.254/o.png" },
      });
    }
    throw new TypeError("fetch failed: " + url);
  };
  try {
    const resp = await proxy.fetch(pngReq({ draw: [{ url: OVER_URL }] }));
    assert.equal(resp.headers.get("cf-resized"), "err=9420");
  } finally {
    globalThis.fetch = prev;
  }
});

test("cf.image draw: overlay follows an allowed redirect hop", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  const { bytes: over } = await eng.encodeImage(
    {
      data: new Uint8ClampedArray(2 * 2 * 4).fill(255),
      width: 2,
      height: 2,
    },
    "png",
    100,
  );
  const prev = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = typeof input === "string" ? input : input.url;
    if (url === PNG_URL) return new Response(await sourcePng(), { status: 200 });
    if (url === OVER_URL) {
      return new Response(null, {
        status: 302,
        headers: { location: "https://cdn.example.com/over-real.png" },
      });
    }
    if (url === "https://cdn.example.com/over-real.png") {
      return new Response(over, { status: 200 });
    }
    throw new TypeError("fetch failed: " + url);
  };
  try {
    const resp = await proxy.fetch(pngReq({ draw: [{ url: OVER_URL }] }));
    assert.equal(resp.status, 200);
  } finally {
    globalThis.fetch = prev;
  }
});

// ---- draw text overlays -------------------------------------------------

const FONT_URL = "https://cdn.example.com/font.ttf";
const FONT_BYTES = new Uint8Array(
  (await import("node:fs")).readFileSync(
    new URL("./fixtures/test-sans.ttf", import.meta.url),
  ),
);

test("cfImagePlan: text draw entry → spec + fontUrl, draw op only", () => {
  const plan = cfImagePlan(
    pngReq({
      draw: [
        { text: "HI", font: { url: FONT_URL }, size: 40, color: "#ff0000", top: 2, left: 4 },
      ],
    }),
  );
  assert.deepEqual(plan.draws, [
    {
      kind: "text",
      spec: '{"text":"HI","size":40,"color":"#ff0000"}',
      fontUrl: FONT_URL,
    },
  ]);
  const ops = JSON.parse(plan.transforms);
  assert.equal(ops.length, 2); // main + draw — text entries emit no transform op
  assert.deepEqual(ops[1], {
    drawImageIndex: 1, targetImageIndex: 0, top: 2, left: 4,
  });
});

test("cfImagePlan: text defaults leave spec minimal; dpr scales offsets", () => {
  const plan = cfImagePlan(
    pngReq({ dpr: 2, draw: [{ text: "T", font: { url: FONT_URL }, top: 5 }] }),
  );
  assert.equal(plan.draws[0].spec, '{"text":"T"}');
  assert.equal(JSON.parse(plan.transforms)[1].top, 10);
});

test("cfImageBody: text entry emits draw_text before draw_font", () => {
  const plan = cfImagePlan(
    pngReq({
      draw: [
        { url: OVER_URL, top: 0 },
        { text: "T", font: { url: FONT_URL }, top: 1 },
      ],
    }),
  );
  const fd = cfImageBody(new Uint8Array([1]), plan, [
    new Uint8Array([2]),
    new Uint8Array([3]),
  ]);
  assert.deepEqual(
    [...fd.entries()].map(([k, v]) => [k, typeof v]),
    [
      ["image", "object"],
      ["transforms", "string"],
      ["output_format", "string"],
      ["draw_image", "object"],
      ["draw_text", "string"],
      ["draw_font", "object"],
    ],
  );
  assert.equal(fd.get("draw_text"), '{"text":"T"}');
});

test("cf.image draw e2e: text overlay composites font-colored glyphs", async () => {
  const base = await sourcePng();
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withUrlMap(
    { [PNG_URL]: [base, 200], [FONT_URL]: [FONT_BYTES, 200] },
    async () => {
      const resp = await proxy.fetch(
        pngReq({
          format: "png",
          draw: [
            { text: "HI", font: { url: FONT_URL }, size: 10, color: "#ff0000", top: 0, left: 0 },
          ],
        }),
      );
      assert.equal(resp.status, 200);
      const out = await eng.decodeImage(
        new Uint8Array(await resp.arrayBuffer()),
        "png",
      );
      let red = 0;
      for (let i = 0; i < out.data.length; i += 4) {
        if (out.data[i] > 200 && out.data[i + 1] < 100) red++;
      }
      assert.ok(red > 0, "glyph pixels composited");
    },
  );
});

test("cf.image draw: unfetchable font → ERROR 9420", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withUrlMap({ [PNG_URL]: [await sourcePng(), 200] }, async () => {
    const resp = await proxy.fetch(
      pngReq({ draw: [{ text: "HI", font: { url: FONT_URL } }] }),
    );
    assert.equal(resp.headers.get("cf-resized"), "err=9420");
    assert.match(await resp.text(), /^ERROR 9420/);
  });
});

test("cf.image draw: garbage font bytes → facade err=9420", async () => {
  const proxy = makeProxy(egressEnv(realImagesFetcher));
  await withUrlMap(
    { [PNG_URL]: [await sourcePng(), 200], [FONT_URL]: [new Uint8Array(64), 200] },
    async () => {
      const resp = await proxy.fetch(
        pngReq({ draw: [{ text: "HI", font: { url: FONT_URL } }] }),
      );
      assert.equal(resp.headers.get("cf-resized"), "err=9420");
    },
  );
});
