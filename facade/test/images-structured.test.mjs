// Structured transform parameters (2026-11-18 "consistent language"
// + 2026-11-19 gaussian sharpen upstream changelogs): object forms of
// sharpen/trim/fit, percentage lengths, new fit modes — asserted on
// real pixels through the vendored engine.
import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule } from "./helpers.mjs";

const { ImagesFacade } = await import(facadeModule("facade-images.js"));
const eng = await import(facadeModule("images-engine.js"));

const facade = new ImagesFacade({}, {});

function frame(width, height, px) {
  const data = new Uint8ClampedArray(width * height * 4);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const i = (y * width + x) * 4;
      const [r, g, b, a] = px(x, y);
      data[i] = r;
      data[i + 1] = g;
      data[i + 2] = b;
      data[i + 3] = a;
    }
  }
  return { data, width, height };
}

const solid = (w, h, c) => frame(w, h, () => c);

/** Mid-tone step edge — sharpening is measurable on both sides. */
function edgeFrame(w, h) {
  return frame(w, h, (x) => (x < w / 2 ? [100, 100, 100, 255] : [160, 160, 160, 255]));
}

async function transform(srcFrame, ops, fields = {}) {
  const src = await eng.encodeImage(srcFrame, "png", 100);
  const form = new FormData();
  form.append("image", new File([src.bytes], "img", { type: "application/octet-stream" }));
  form.append("transforms", JSON.stringify(ops));
  form.append("output_format", fields.output_format ?? "image/png");
  const resp = await facade.fetch(
    new Request("https://js.images.cloudflare.com/transform", { method: "POST", body: form }),
  );
  return resp;
}

function errCode(resp) {
  const m = /err=(\d+)/.exec(resp.headers.get("cf-images-binding") ?? "");
  return m ? Number(m[1]) : null;
}

async function decode(resp) {
  assert.equal(resp.status, 200);
  return eng.decodeImage(new Uint8Array(await resp.arrayBuffer()), "png");
}

const px = (img, x, y) => [...img.data.subarray((y * img.width + x) * 4, (y * img.width + x) * 4 + 4)];

test("sharpen scalar → unsharp on the step edge; object box equals it", async () => {
  const base = edgeFrame(16, 16);
  const plain = await transform(base, [{ imageIndex: 0, sharpen: 5 }]);
  const plainImg = await decode(plain);
  // Dark side just left of the edge gets darker, bright side brighter.
  assert.ok(px(plainImg, 7, 8)[0] < 100, `left edge px ${px(plainImg, 7, 8)}`);
  assert.ok(px(plainImg, 8, 8)[0] > 160, `right edge px ${px(plainImg, 8, 8)}`);
  // Far from the edge nothing changes.
  assert.equal(px(plainImg, 1, 8)[0], 100);
  assert.equal(px(plainImg, 14, 8)[0], 160);

  const boxed = await transform(base, [{ imageIndex: 0, sharpen: { mode: "box", amount: 5 } }]);
  const boxImg = await decode(boxed);
  assert.deepEqual(px(boxImg, 7, 8), px(plainImg, 7, 8));
  assert.deepEqual(px(boxImg, 8, 8), px(plainImg, 8, 8));
});

test("sharpen gaussian: different kernel, threshold gates weak edges", async () => {
  const base = edgeFrame(16, 16);
  const gauss = await transform(base, [
    { imageIndex: 0, sharpen: { mode: "gaussian", amount: 1, sigma: 1.5 } },
  ]);
  const g = await decode(gauss);
  assert.ok(px(g, 7, 8)[0] < 100, `gaussian sharpens dark side ${px(g, 7, 8)}`);
  assert.ok(px(g, 8, 8)[0] > 160, `gaussian sharpens bright side ${px(g, 8, 8)}`);

  const box = await transform(base, [
    { imageIndex: 0, sharpen: { mode: "box", amount: 1 } },
  ]);
  const b = await decode(box);
  assert.notDeepEqual(px(g, 6, 8), px(b, 6, 8), "gaussian kernel spreads wider than 3x3 box");

  // threshold above the edge difference → pixels pass through unsharpened.
  const gated = await transform(base, [
    { imageIndex: 0, sharpen: { mode: "gaussian", amount: 10, sigma: 1.5, threshold: 200 } },
  ]);
  const gi = await decode(gated);
  assert.equal(px(gi, 7, 8)[0], 100);
  assert.equal(px(gi, 8, 8)[0], 160);
});

test("sharpen object validation fails closed", async () => {
  for (const sharpen of [
    { mode: "gaussian", amount: 1 }, // sigma required
    { mode: "gaussian", amount: 1, sigma: 0 }, // sigma must be > 0
    { mode: "gaussian", amount: 1, sigma: 1, threshold: 300 },
    { mode: "box", amount: 1, sigma: 1 }, // sigma is gaussian-only
    { amount: 1, bogus: 1 },
    { mode: "mild", amount: 1 },
    {}, // amount required in object form
    "sharp",
  ]) {
    const resp = await transform(edgeFrame(8, 8), [{ imageIndex: 0, sharpen }]);
    assert.equal(errCode(resp), 9423, JSON.stringify(sharpen));
  }
});

test("trim color: auto border, custom color, tolerance, keep", async () => {
  // 4px red border around an 8x8 blue center.
  const bordered = frame(16, 16, (x, y) =>
    x < 4 || x >= 12 || y < 4 || y >= 12 ? [255, 0, 0, 255] : [0, 0, 255, 255]);
  const auto = await transform(bordered, [{ imageIndex: 0, trim: { mode: "color" } }]);
  const a = await decode(auto);
  assert.equal(a.width, 8);
  assert.equal(a.height, 8);
  assert.deepEqual(px(a, 0, 0).slice(0, 3), [0, 0, 255]);

  // Same via the URL-era string forms.
  for (const t of ["border", "color"]) {
    const r = await transform(bordered, [{ imageIndex: 0, trim: t }]);
    const ri = await decode(r);
    assert.equal(ri.width, 8, `trim=${t}`);
  }

  // keep=2 leaves a red margin.
  const keep = await transform(bordered, [{ imageIndex: 0, trim: { mode: "color", keep: 2 } }]);
  const k = await decode(keep);
  assert.equal(k.width, 12);
  assert.deepEqual(px(k, 0, 0).slice(0, 3), [255, 0, 0]);

  // Explicit color + tolerance: border is off-red within slack.
  const offRed = frame(16, 16, (x, y) =>
    x < 4 || x >= 12 || y < 4 || y >= 12 ? [250, 10, 10, 255] : [0, 0, 255, 255]);
  const loose = await transform(offRed, [
    { imageIndex: 0, trim: { mode: "color", color: "#ff0000", tolerance: 20 } },
  ]);
  assert.equal((await decode(loose)).width, 8);
  const strict = await transform(offRed, [
    { imageIndex: 0, trim: { mode: "color", color: "#ff0000", tolerance: 0 } },
  ]);
  // tolerance 0 = exact match only → nothing trims.
  assert.equal((await decode(strict)).width, 16);
});

test("trim area: px sides, percents, fractions, rect, bare inference", async () => {
  const src = frame(100, 50, () => [10, 20, 30, 255]);
  const dec = async (trim) =>
    eng.decodeImage(
      new Uint8Array(await (await transform(src, [{ imageIndex: 0, trim }])).arrayBuffer()),
      "png",
    );

  const sides = await dec({ mode: "area", top: 5, right: 10, bottom: 5, left: 10 });
  assert.deepEqual([sides.width, sides.height], [80, 40]);

  const pct = await dec({ mode: "area", top: "10p", right: "20p", bottom: "10p", left: "20p" });
  assert.deepEqual([pct.width, pct.height], [60, 40]);

  const frac = await dec({ mode: "area", top: 0.1, right: 0.2, bottom: 0.1, left: 0.2 });
  assert.deepEqual([frac.width, frac.height], [60, 40]);

  const rect = await dec({ mode: "area", left: 10, top: 10, width: 50, height: 30 });
  assert.deepEqual([rect.width, rect.height], [50, 30]);

  // Bare side key infers area mode.
  const bare = await dec({ left: 8 });
  assert.deepEqual([bare.width, bare.height], [92, 50]);

  // Rect exceeding the source fails closed.
  const bad = await transform(src, [
    { imageIndex: 0, trim: { mode: "area", left: 80, top: 40, width: 50, height: 30 } },
  ]);
  assert.equal(errCode(bad), 9423);
});

test("fit aspect-crop crops to the target aspect without upscaling", async () => {
  const src = frame(16, 8, () => [10, 20, 30, 255]);
  // Square target on a 2:1 source → center 8x8.
  const sq = await transform(src, [{ imageIndex: 0, width: 8, height: 8, fit: "aspect-crop" }]);
  const s = await decode(sq);
  assert.deepEqual([s.width, s.height], [8, 8]);

  // Target larger than source → no upscale; output keeps cropped
  // source dims (not the requested box).
  const big = await transform(src, [{ imageIndex: 0, width: 32, height: 32, fit: "aspect-crop" }]);
  const b = await decode(big);
  assert.deepEqual([b.width, b.height], [8, 8]);

  // Gravity steers the crop window: left half is red.
  const half = frame(16, 8, (x) => (x < 8 ? [255, 0, 0, 255] : [0, 0, 255, 255]));
  const left = await transform(half, [
    { imageIndex: 0, width: 8, height: 8, fit: "aspect-crop", gravity: "left" },
  ]);
  const l = await decode(left);
  assert.deepEqual(px(l, 0, 0).slice(0, 3), [255, 0, 0]);
});

test("fit scale-up enlarges but never shrinks", async () => {
  const src = frame(8, 8, () => [10, 20, 30, 255]);
  const up = await transform(src, [{ imageIndex: 0, width: 16, height: 16, fit: "scale-up" }]);
  const u = await decode(up);
  assert.deepEqual([u.width, u.height], [16, 16]);
  const down = await transform(src, [{ imageIndex: 0, width: 4, height: 4, fit: "scale-up" }]);
  const d = await decode(down);
  assert.deepEqual([d.width, d.height], [8, 8]);
});

test("fit object form: upscale gate fails closed on generate only", async () => {
  const src = frame(8, 8, () => [10, 20, 30, 255]);
  const ok = await transform(src, [
    { imageIndex: 0, width: 4, height: 4, fit: { mode: "contain", upscale: "interpolate" } },
  ]);
  const o = await decode(ok);
  assert.deepEqual([o.width, o.height], [4, 4]);
  const gen = await transform(src, [
    { imageIndex: 0, width: 16, height: 16, fit: { mode: "contain", upscale: "generate" } },
  ]);
  assert.equal(errCode(gen), 9401);
  const bogus = await transform(src, [
    { imageIndex: 0, fit: { mode: "stretch" } },
  ]);
  assert.equal(errCode(bogus), 9423);
});

test("flip h/v/hv mirrors pixels", async () => {
  const src = frame(4, 2, (x, y) => (x === 0 && y === 0 ? [255, 0, 0, 255] : [0, 0, 255, 255]));
  const red = (img, x, y) => px(img, x, y)[0] === 255;
  for (const [flip, want] of [["h", [3, 0]], ["v", [0, 1]], ["hv", [3, 1]]]) {
    const resp = await transform(src, [{ imageIndex: 0, flip }]);
    const img = await decode(resp);
    assert.ok(red(img, want[0], want[1]), `flip=${flip} red at ${want}`);
    assert.equal([0, 1, 2, 3].reduce((n, x) => n + (red(img, x, 0) ? 1 : 0), 0) +
      (red(img, 0, 1) || red(img, 1, 1) || red(img, 2, 1) || red(img, 3, 1) ? 1 : 0), 1);
  }
});

test("border accepts percent widths resolved per axis", async () => {
  const src = frame(20, 10, () => [0, 0, 255, 255]);
  const resp = await transform(src, [
    { imageIndex: 0, border: { width: "10p", color: "#ff0000" } },
  ]);
  const img = await decode(resp);
  // 10% of each side's own axis: +2px sides (20), +1px top/bottom (10).
  assert.deepEqual([img.width, img.height], [24, 12]);
  assert.deepEqual(px(img, 0, 0).slice(0, 3), [255, 0, 0]);
  assert.deepEqual(px(img, 2, 1).slice(0, 3), [0, 0, 255]);
});
