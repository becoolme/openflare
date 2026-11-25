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
