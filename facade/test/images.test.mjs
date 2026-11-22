import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule } from "./helpers.mjs";

const { ImagesFacade } = await import(facadeModule("facade-images.js"));
const { sniffImage, sniffSvg } = await import(facadeModule("images-wire.js"));

const facade = new ImagesFacade({}, {});

function png({ width = 3, height = 2 } = {}) {
  const buf = new Uint8Array(33);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0x89504e47);
  dv.setUint32(4, 0x0d0a1a0a);
  dv.setUint32(16, width);
  dv.setUint32(20, height);
  return buf;
}

/** Solid-color RGBA frame for engine-encoded fixtures. */
function solidFrame(width, height, [r, g, b, a] = [10, 20, 30, 255]) {
  const data = new Uint8ClampedArray(width * height * 4);
  for (let i = 0; i < data.length; i += 4) {
    data[i] = r;
    data[i + 1] = g;
    data[i + 2] = b;
    data[i + 3] = a;
  }
  return { data, width, height };
}

function gif({ width = 5, height = 4 } = {}) {
  const buf = new Uint8Array(10);
  buf.set([0x47, 0x49, 0x46, 0x38, 0x39, 0x61]);
  new DataView(buf.buffer).setUint16(6, width, true);
  new DataView(buf.buffer).setUint16(8, height, true);
  return buf;
}

function jpeg({ width = 7, height = 6 } = {}) {
  // SOI, APP0 (len 16), SOF0 carrying the dims.
  const buf = new Uint8Array(32);
  const dv = new DataView(buf.buffer);
  dv.setUint16(0, 0xffd8);
  dv.setUint16(2, 0xffe0);
  dv.setUint16(4, 16);
  dv.setUint16(20, 0xffc0);
  dv.setUint16(22, 9);
  dv.setUint16(25, height);
  dv.setUint16(27, width);
  return buf;
}

function webpVp8x({ width = 300, height = 200 } = {}) {
  const buf = new Uint8Array(30);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0x52494646); // RIFF
  dv.setUint32(8, 0x57454250); // WEBP
  dv.setUint32(12, 0x56503858); // VP8X
  const w = width - 1;
  const h = height - 1;
  buf[24] = w & 0xff; buf[25] = (w >> 8) & 0xff; buf[26] = (w >> 16) & 0xff;
  buf[27] = h & 0xff; buf[28] = (h >> 8) & 0xff; buf[29] = (h >> 16) & 0xff;
  return buf;
}

function post(pathname, fields) {
  const form = new FormData();
  for (const [k, v] of Object.entries(fields)) {
    form.append(k, v);
  }
  return facade.fetch(
    new Request(`https://js.images.cloudflare.com${pathname}`, {
      method: "POST",
      body: form,
    }),
  );
}

function imageField(bytes) {
  return new File([bytes], "img", { type: "application/octet-stream" });
}

function errCode(resp) {
  const m = /err=(\d+)/.exec(resp.headers.get("cf-images-binding") ?? "");
  return m ? Number(m[1]) : null;
}

test("sniffImage parses PNG/GIF/JPEG/WebP dimensions", () => {
  assert.deepEqual(sniffImage(png({ width: 640, height: 480 })), {
    format: "png", width: 640, height: 480,
  });
  assert.deepEqual(sniffImage(gif({ width: 11, height: 13 })), {
    format: "gif", width: 11, height: 13,
  });
  assert.deepEqual(sniffImage(jpeg({ width: 1024, height: 768 })), {
    format: "jpeg", width: 1024, height: 768,
  });
  assert.deepEqual(sniffImage(webpVp8x({ width: 300, height: 200 })), {
    format: "webp", width: 300, height: 200,
  });
});

test("sniffImage rejects truncated and unrecognized buffers", () => {
  assert.equal(sniffImage(new Uint8Array(0)), null);
  assert.equal(sniffImage(png().subarray(0, 12)), null);
  assert.equal(
    sniffImage(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26])),
    null,
  );
});

test("sniffSvg matches the upstream dim-less shape condition", () => {
  assert.ok(sniffSvg(new TextEncoder().encode('  <svg xmlns="x"/>')));
  assert.ok(sniffSvg(new TextEncoder().encode('<?xml version="1.0"?><svg/>')));
  assert.ok(!sniffSvg(png()));
});

test("POST /info returns the upstream snake_case shape for a PNG", async () => {
  const resp = await post("/info", { image: imageField(png({ width: 640, height: 480 })) });
  assert.equal(resp.status, 200);
  const info = await resp.json();
  // Wire shape is snake_case — the upstream shim maps file_size → fileSize.
  assert.equal(info.format, "image/png");
  assert.equal(info.width, 640);
  assert.equal(info.height, 480);
  assert.equal(info.file_size, 33);
});

test("POST /info on SVG markup returns the dim-less shape", async () => {
  const resp = await post("/info", {
    image: imageField(new TextEncoder().encode("<svg xmlns='x'><rect/></svg>")),
  });
  assert.equal(resp.status, 200);
  assert.deepEqual(await resp.json(), { format: "image/svg+xml" });
});

test("non-POST methods fail closed with the upstream error header", async () => {
  const resp = await facade.fetch(new Request("https://js.images.cloudflare.com/info"));
  assert.equal(resp.status, 405);
  assert.equal(errCode(resp), 9406);
});

test("corrupt body → 415 err=9420; unknown op → 404", async () => {
  const resp = await post("/info", { image: imageField(new Uint8Array([1, 2, 3])) });
  assert.equal(resp.status, 415);
  assert.equal(errCode(resp), 9420);
  const unknown = await post("/bogus", { image: imageField(png()) });
  assert.equal(unknown.status, 404);
  assert.equal(errCode(unknown), 9404);
});

test("missing image field → 400 err=9423", async () => {
  const resp = await post("/info", { transforms: "[]" });
  assert.equal(resp.status, 400);
  assert.equal(errCode(resp), 9423);
});

test("transform: PNG → scale-down → webp round-trips through the vendored engine", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const src = await eng.encodeImage(solidFrame(16, 16), "png", 90);
  const resp = await post("/transform", {
    image: imageField(src.bytes),
    transforms: JSON.stringify([{ imageIndex: 0, width: 8, height: 8 }]),
    output_format: "image/webp",
  });
  assert.equal(resp.status, 200);
  assert.equal(resp.headers.get("content-type"), "image/webp");
  const dec = await eng.decodeImage(new Uint8Array(await resp.arrayBuffer()), "webp");
  assert.equal(dec.width, 8);
  assert.equal(dec.height, 8);
});

test("transform: crop-gravity + rotate → jpeg", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const src = await eng.encodeImage(solidFrame(16, 16), "png", 90);
  const resp = await post("/transform", {
    image: imageField(src.bytes),
    transforms: JSON.stringify([
      { imageIndex: 0, width: 8, height: 16, fit: "crop", gravity: "left" },
      { imageIndex: 0, rotate: 90 },
    ]),
    output_format: "image/jpeg",
    output_quality: "80",
  });
  assert.equal(resp.status, 200);
  const dec = await eng.decodeImage(new Uint8Array(await resp.arrayBuffer()), "jpeg");
  assert.equal(dec.width, 16);
  assert.equal(dec.height, 8);
});

test("transform: draw_image overlay composites at 50% opacity", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const src = await eng.encodeImage(solidFrame(16, 16, [200, 0, 0, 255]), "png", 90);
  const over = await eng.encodeImage(solidFrame(4, 4, [255, 255, 255, 255]), "png", 90);
  const resp = await post("/transform", {
    image: imageField(src.bytes),
    draw_image: imageField(over.bytes),
    transforms: JSON.stringify([
      { drawImageIndex: 1, targetImageIndex: 0, left: 0, top: 0, opacity: 0.5 },
    ]),
    output_format: "image/png",
  });
  assert.equal(resp.status, 200);
  const dec = await eng.decodeImage(new Uint8Array(await resp.arrayBuffer()), "png");
  // 0.5·255 over 1.0·200 → 227.5 → 227/228.
  assert.ok(dec.data[0] > 220 && dec.data[0] < 235, `alpha composited red channel ${dec.data[0]}`);
});

test("transform: unknown transform key fails closed err=9423", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const src = await eng.encodeImage(solidFrame(8, 8), "png", 90);
  const resp = await post("/transform", {
    image: imageField(src.bytes),
    transforms: JSON.stringify([{ imageIndex: 0, bogus: 1 }]),
    output_format: "image/png",
  });
  assert.equal(resp.status, 400);
  assert.equal(errCode(resp), 9423);
});

test("transform: gif input fails closed err=9401 (no vendored decoder)", async () => {
  const resp = await post("/transform", {
    image: imageField(gif()),
    transforms: "[]",
    output_format: "image/png",
  });
  assert.equal(resp.status, 415);
  assert.equal(errCode(resp), 9401);
});

test("transform: text_input/draw_text fail closed err=9401", async () => {
  const a = await post("/transform", {
    text_input: JSON.stringify({ text: "hi", font: "x" }),
    transforms: "[]",
    output_format: "image/png",
  });
  assert.equal(errCode(a), 9401);
});

test("transform: avif/gif output_format fails closed err=9401", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const src = await eng.encodeImage(solidFrame(8, 8), "png", 90);
  for (const output_format of ["image/avif", "image/gif"]) {
    const resp = await post("/transform", {
      image: imageField(src.bytes),
      transforms: "[]",
      output_format,
    });
    assert.equal(resp.status, 415, output_format);
    assert.equal(errCode(resp), 9401);
  }
});

test("malformed/missing transforms JSON → 400 err=9423", async () => {
  for (const transforms of ["{not json", "{}", undefined]) {
    const fields = { image: imageField(png()) };
    if (transforms !== undefined) fields.transforms = transforms;
    const resp = await post("/transform", fields);
    assert.equal(resp.status, 400, `transforms=${transforms}`);
    assert.equal(errCode(resp), 9423);
  }
});
