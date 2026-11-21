import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule } from "./helpers.mjs";

const { ImagesFacade } = await import(facadeModule("facade-images.js"));
const { sniffImage, IMAGES_PARAMS_HEADER } = await import(
  facadeModule("images-wire.js")
);

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

async function post(pathname, body, headers = {}) {
  return facade.fetch(
    new Request(`http://images${pathname}`, { method: "POST", headers, body }),
  );
}

async function errorCode(resp) {
  return (await resp.json()).error.code;
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
  assert.equal(sniffImage(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26])), null);
});

test("POST /info returns CF info() shape for a PNG", async () => {
  const resp = await post("/info", png({ width: 640, height: 480 }));
  assert.equal(resp.status, 200);
  const info = await resp.json();
  assert.equal(info.format, "image/png");
  assert.equal(info.width, 640);
  assert.equal(info.height, 480);
  assert.equal(info.fileSize, 33);
});

test("non-POST methods fail closed", async () => {
  const resp = await facade.fetch(new Request("http://images/info"));
  assert.equal(resp.status, 405);
  assert.equal(await errorCode(resp), "9406");
});

test("corrupt body → 415 9420; unknown op → 404", async () => {
  const resp = await post("/info", new Uint8Array([1, 2, 3]));
  assert.equal(resp.status, 415);
  assert.equal(await errorCode(resp), "9420");
  const unknown = await post("/bogus", png());
  assert.equal(unknown.status, 404);
});

test("empty body is rejected before sniffing", async () => {
  const resp = await post("/info", new Uint8Array(0));
  assert.equal(resp.status, 413);
  assert.equal(await errorCode(resp), "9409");
});

test("transform fails closed 501 until the engine is vendored", async () => {
  const resp = await post("/transform", png(), {
    [IMAGES_PARAMS_HEADER]: JSON.stringify({ width: 10 }),
  });
  assert.equal(resp.status, 501);
  assert.equal(await errorCode(resp), "9490");
});

test("malformed transform params → 400 9423", async () => {
  const resp = await post("/transform", png(), {
    [IMAGES_PARAMS_HEADER]: "{not json",
  });
  assert.equal(resp.status, 400);
  assert.equal(await errorCode(resp), "9423");
});
