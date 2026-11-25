import "./shims/register.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { facadeModule } from "./helpers.mjs";

const { ImagesFacade } = await import(facadeModule("facade-images.js"));
const { extractMetadata, attachMetadata, iptcToXmp } = await import(
  facadeModule("images-metadata.js")
);

const facade = new ImagesFacade({}, {});
const enc = new TextEncoder();

function solidFrame(width, height, [r, g, b, a] = [10, 20, 30, 255]) {
  const data = new Uint8ClampedArray(width * height * 4);
  for (let i = 0; i < data.length; i += 4) {
    data[i] = r; data[i + 1] = g; data[i + 2] = b; data[i + 3] = a;
  }
  return { data, width, height };
}

function post(pathname, fields) {
  const form = new FormData();
  for (const [k, v] of Object.entries(fields)) form.append(k, v);
  return facade.fetch(
    new Request(`https://js.images.cloudflare.com${pathname}`, {
      method: "POST",
      body: form,
    }),
  );
}

function errCode(resp) {
  const m = /err=(\d+)/.exec(resp.headers.get("cf-images-binding") ?? "");
  return m ? Number(m[1]) : null;
}

// ---- metadata fixture builders ----

/** Minimal LE TIFF with a Copyright tag (0x8298) + one extra ASCII tag
 * (Make, 0x010f) so `copyright` filtering is observable. */
function tiffWithCopyright() {
  const cpr = enc.encode("(C) 2026 ACME\0");
  const mk = enc.encode("DEVCAM\0");
  const out = new Uint8Array(8 + 2 + 24 + 4 + cpr.length + mk.length);
  const dv = new DataView(out.buffer);
  out.set([0x49, 0x49]);
  dv.setUint16(2, 42, true);
  dv.setUint32(4, 8, true);
  dv.setUint16(8, 2, true);
  // Copyright 0x8298, ASCII, inline offset → data area after IFD.
  const dataOff = 8 + 2 + 24 + 4;
  dv.setUint16(10, 0x8298, true); dv.setUint16(12, 2, true);
  dv.setUint32(14, cpr.length, true); dv.setUint32(18, dataOff, true);
  dv.setUint16(22, 0x010f, true); dv.setUint16(24, 2, true);
  dv.setUint32(26, mk.length, true); dv.setUint32(30, dataOff + cpr.length, true);
  dv.setUint32(34, 0, true);
  out.set(cpr, dataOff);
  out.set(mk, dataOff + cpr.length);
  return out;
}

function xmpPacket(body = "<rdf:li>x</rdf:li>") {
  return enc.encode(
    '<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF ' +
      'xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">' +
      `<rdf:Description rdf:about="">${body}</rdf:Description>` +
      "</rdf:RDF></x:xmpmeta>",
  );
}

/** 8BIM block: resource 0x0404 wrapping IIM record-2 datasets. */
function iptcBlock(datasets = [[0x74, "(C) IPTC-RIGHTS"], [0x50, "photog"]]) {
  let iimLen = 0;
  for (const [, v] of datasets) iimLen += 5 + enc.encode(v).length;
  const iim = new Uint8Array(iimLen);
  const idv = new DataView(iim.buffer);
  let q = 0;
  for (const [ds, v] of datasets) {
    const vb = enc.encode(v);
    iim[q] = 0x1c; iim[q + 1] = 2; iim[q + 2] = ds;
    idv.setUint16(q + 3, vb.length);
    iim.set(vb, q + 5);
    q += 5 + vb.length;
  }
  const namePad = 2; // empty Pascal name (len byte + pad byte)
  const out = new Uint8Array(4 + 2 + namePad + 4 + iim.length + (iim.length & 1));
  const dv = new DataView(out.buffer);
  out.set(enc.encode("8BIM"), 0);
  dv.setUint16(4, 0x0404);
  // bytes 6-7: name length 0 + pad (already zero)
  dv.setUint32(8, iim.length);
  out.set(iim, 12);
  return out;
}

function jpegSeg(marker, payload) {
  const out = new Uint8Array(4 + payload.length);
  const dv = new DataView(out.buffer);
  out[0] = 0xff; out[1] = marker;
  dv.setUint16(2, payload.length + 2);
  out.set(payload, 4);
  return out;
}

const EXIF_PFX = [0x45, 0x78, 0x69, 0x66, 0, 0];
const XMP_PFX = enc.encode("http://ns.adobe.com/xap/1.0/\0");
const PS_PFX = enc.encode("Photoshop 3.0\0");

/** Splice APPn metadata segments into an encoded JPEG, right after SOI. */
function jpegWithMeta(jpeg, { exif, xmp, iptc } = {}) {
  const segs = [];
  if (exif) {
    const p = new Uint8Array(6 + exif.length);
    p.set(EXIF_PFX); p.set(exif, 6);
    segs.push(jpegSeg(0xe1, p));
  }
  if (xmp) {
    const p = new Uint8Array(XMP_PFX.length + xmp.length);
    p.set(XMP_PFX); p.set(xmp, XMP_PFX.length);
    segs.push(jpegSeg(0xe1, p));
  }
  if (iptc) {
    const p = new Uint8Array(PS_PFX.length + iptc.length);
    p.set(PS_PFX); p.set(iptc, PS_PFX.length);
    segs.push(jpegSeg(0xed, p));
  }
  const total = segs.reduce((s, x) => s + x.length, 0);
  const out = new Uint8Array(jpeg.length + total);
  out.set(jpeg.subarray(0, 2));
  let p = 2;
  for (const s of segs) { out.set(s, p); p += s.length; }
  out.set(jpeg.subarray(2), p);
  return out;
}

/** Find all marker-0xAPPn segment payloads in a JPEG (payload sans header). */
function jpegSegments(buf, marker) {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const found = [];
  let off = 2;
  while (off + 4 <= buf.length && buf[off] === 0xff) {
    const m = buf[off + 1];
    if (m === 0xda || m === 0xd9) break;
    const len = dv.getUint16(off + 2);
    if (m === marker) found.push(buf.slice(off + 4, off + 2 + len));
    off += 2 + len;
  }
  return found;
}

function pngChunks(buf, type) {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const found = [];
  let off = 8;
  while (off + 12 <= buf.length) {
    const len = dv.getUint32(off);
    const t = String.fromCharCode(...buf.subarray(off + 4, off + 8));
    if (t === type) found.push(buf.slice(off + 8, off + 8 + len));
    off = off + 8 + len + 4;
  }
  return found;
}

function riffChunks(buf, fourcc) {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const found = [];
  let off = 12;
  while (off + 8 <= buf.length) {
    const len = dv.getUint32(off + 4, true);
    const t = String.fromCharCode(...buf.subarray(off, off + 4));
    if (t === fourcc) found.push(buf.slice(off + 8, off + 8 + len));
    off = off + 8 + len + (len & 1);
  }
  return found;
}

async function transform(imageBytes, fields = {}) {
  const resp = await post("/transform", {
    image: new File([imageBytes], "img", { type: "application/octet-stream" }),
    transforms: "[]",
    output_format: "image/jpeg",
    ...fields,
  });
  assert.equal(resp.status, 200, `status ${resp.status}`);
  return new Uint8Array(await resp.arrayBuffer());
}

// ---- extraction unit tests ----

test("extractMetadata: JPEG APPn segments", () => {
  const tiff = tiffWithCopyright();
  const xmp = xmpPacket();
  const iptc = iptcBlock();
  const fake = jpegWithMeta(
    new Uint8Array([0xff, 0xd8, 0xff, 0xd9]),
    { exif: tiff, xmp, iptc },
  );
  const meta = extractMetadata(fake, "jpeg");
  assert.deepEqual(meta.exif, tiff);
  assert.deepEqual(meta.xmp, xmp);
  assert.deepEqual(meta.iptc, iptc);
});

test("extractMetadata: PNG eXIf + iTXt xmp", () => {
  const tiff = tiffWithCopyright();
  const xmp = xmpPacket();
  // Build iTXt: keyword\0 flag method lang\0 trans\0 text
  const kw = enc.encode("XML:com.adobe.xmp");
  const itxt = new Uint8Array(kw.length + 1 + 2 + 1 + 1 + xmp.length);
  itxt.set(kw); itxt.set(xmp, kw.length + 5);
  const chunk = (type, data) => {
    const c = new Uint8Array(12 + data.length);
    const dv = new DataView(c.buffer);
    dv.setUint32(0, data.length);
    c.set(enc.encode(type), 4);
    c.set(data, 8);
    return c;
  };
  const ihdr = new Uint8Array(13);
  const png = new Uint8Array(8);
  const dv8 = new DataView(png.buffer);
  dv8.setUint32(0, 0x89504e47); dv8.setUint32(4, 0x0d0a1a0a);
  const full = new Uint8Array(8 + 25 + 12 + tiff.length + 12 + itxt.length + 12);
  let p = 0;
  full.set(png, 0); p += 8;
  full.set(chunk("IHDR", ihdr), p); p += 25;
  full.set(chunk("eXIf", tiff), p); p += 12 + tiff.length;
  full.set(chunk("iTXt", itxt), p); p += 12 + itxt.length;
  full.set(chunk("IEND", new Uint8Array(0)), p);
  const meta = extractMetadata(full, "png");
  assert.deepEqual(meta.exif, tiff);
  assert.deepEqual(meta.xmp, xmp);
});

test("iptcToXmp maps record-2 fields to dc:/photoshop: namespaces", () => {
  const xmp = new TextDecoder().decode(iptcToXmp(iptcBlock()));
  assert.match(xmp, /dc:rights/);
  assert.match(xmp, /IPTC-RIGHTS/);
  assert.match(xmp, /dc:creator/);
  assert.match(xmp, /photog/);
  assert.match(xmp, /rdf:Alt|rdf:Seq/);
});

// ---- wire-level round-trips ----

test("transform metadata=keep: jpeg→jpeg preserves EXIF+XMP+IPTC", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "jpeg", 90);
  const tiff = tiffWithCopyright();
  const xmp = xmpPacket();
  const iptc = iptcBlock();
  const src = jpegWithMeta(base.bytes, { exif: tiff, xmp, iptc });

  const out = await transform(src, { metadata: "keep" });
  const app1 = jpegSegments(out, 0xe1);
  const app13 = jpegSegments(out, 0xed);
  assert.ok(app1.some((s) => s[0] === 0x45 && s[1] === 0x78), "EXIF APP1 present");
  assert.ok(
    app1.some((s) => s[0] === 0x68 && s[1] === 0x74), // "ht" of the XMP ns
    "XMP APP1 present",
  );
  assert.ok(
    app13.some((s) => s[0] === 0x50 && s[1] === 0x68), // "Ph" of Photoshop
    "IPTC APP13 present",
  );
  // EXIF payload byte-for-byte.
  const exifSeg = app1.find((s) => s[0] === 0x45);
  assert.deepEqual(exifSeg.subarray(6), tiff);
});

test("transform metadata=keep: jpeg→webp writes EXIF+XMP chunks, IPTC→XMP skipped when XMP exists", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "jpeg", 90);
  const src = jpegWithMeta(base.bytes, {
    exif: tiffWithCopyright(),
    xmp: xmpPacket("<rdf:li>orig</rdf:li>"),
    iptc: iptcBlock(),
  });
  const out = await transform(src, {
    metadata: "keep",
    output_format: "image/webp",
  });
  assert.deepEqual([...out.subarray(8, 12)], [...enc.encode("WEBP")]);
  assert.ok(riffChunks(out, "EXIF").length === 1, "EXIF chunk");
  const xmps = riffChunks(out, "XMP ");
  assert.equal(xmps.length, 1);
  // Source already has XMP → IPTC not merged; original XMP survives.
  const text = new TextDecoder().decode(xmps[0]);
  assert.ok(text.includes("orig"), "original XMP kept");
  assert.ok(!text.includes("IPTC-RIGHTS"), "no IPTC merge into existing XMP");
});

test("transform metadata=keep: jpeg→png writes eXIf+iTXt, IPTC→XMP when no source XMP", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "jpeg", 90);
  const src = jpegWithMeta(base.bytes, {
    exif: tiffWithCopyright(),
    iptc: iptcBlock(),
  });
  const out = await transform(src, {
    metadata: "keep",
    output_format: "image/png",
  });
  assert.equal(pngChunks(out, "eXIf").length, 1, "eXIf chunk");
  const itxts = pngChunks(out, "iTXt");
  assert.equal(itxts.length, 1);
  const text = new TextDecoder().decode(itxts[0]);
  assert.ok(text.includes("XML:com.adobe.xmp"), "iTXt xmp keyword");
  assert.ok(text.includes("IPTC-RIGHTS"), "IPTC copyright mapped to XMP");
});

test("transform metadata=copyright keeps only the Copyright tag; metadata=none strips all", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "jpeg", 90);
  const src = jpegWithMeta(base.bytes, {
    exif: tiffWithCopyright(),
    xmp: xmpPacket(),
  });

  const cpr = await transform(src, { metadata: "copyright" });
  const app1 = jpegSegments(cpr, 0xe1);
  const exifSeg = app1.find((s) => s[0] === 0x45);
  assert.ok(exifSeg, "EXIF kept");
  const tiff = exifSeg.subarray(6);
  const tdv = new DataView(tiff.buffer, tiff.byteOffset, tiff.byteLength);
  assert.equal(tdv.getUint16(8, true), 1, "single tag rebuilt");
  assert.equal(tdv.getUint16(10, true), 0x8298, "tag is Copyright");
  assert.ok(
    !app1.some((s) => s[0] === 0x68 && s[1] === 0x74),
    "XMP stripped under copyright",
  );

  const none = await transform(src, { metadata: "none" });
  assert.equal(jpegSegments(none, 0xe1).length, 0, "no APP1 under none");
});

test("transform: absent metadata field defaults to copyright (upstream)", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "jpeg", 90);
  const src = jpegWithMeta(base.bytes, { exif: tiffWithCopyright() });
  const out = await transform(src);
  const app1 = jpegSegments(out, 0xe1);
  const exifSeg = app1.find((s) => s[0] === 0x45);
  assert.ok(exifSeg, "copyright EXIF kept by default");
  const tiff = exifSeg.subarray(6);
  assert.equal(new DataView(tiff.buffer, tiff.byteOffset, tiff.byteLength).getUint16(10, true), 0x8298);
});

test("transform: invalid metadata value fails closed err=9423", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const src = await eng.encodeImage(solidFrame(4, 4), "png", 90);
  const resp = await post("/transform", {
    image: new File([src.bytes], "img"),
    transforms: "[]",
    output_format: "image/jpeg",
    metadata: "everything",
  });
  assert.equal(resp.status, 400);
  assert.equal(errCode(resp), 9423);
});

test("attachMetadata: rgb/rgba pass through untouched", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "png", 90);
  const meta = { exif: tiffWithCopyright(), xmp: xmpPacket() };
  for (const format of ["rgb", "rgba"]) {
    const out = attachMetadata(base.bytes, format, meta, "keep", 8, 8);
    assert.equal(out, base.bytes, `${format} untouched`);
  }
});

test("transform metadata=keep: jpeg→avif embeds an Exif item (XMP dropped — upstream writes EXIF only)", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "jpeg", 90);
  const tiff = tiffWithCopyright();
  const src = jpegWithMeta(base.bytes, { exif: tiff, xmp: xmpPacket() });
  const out = await transform(src, {
    metadata: "keep",
    output_format: "image/avif",
  });
  const meta = extractMetadata(out, "avif");
  assert.deepEqual(meta.exif, tiff, "Exif item round-trips");
  assert.equal(meta.xmp, undefined, "AVIF write is EXIF-only upstream");
  // Patched container still decodes.
  const dec = await eng.decodeImage(out, "avif");
  assert.equal(dec.width, 8);
  assert.equal(dec.height, 8);
});

test("extractMetadata: avif source reads back a written Exif item", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "avif", 80);
  const tiff = tiffWithCopyright();
  const withMeta = attachMetadata(base.bytes, "avif", { exif: tiff }, "keep", 8, 8);
  assert.notEqual(withMeta.length, base.bytes.length, "file grew");
  const meta = extractMetadata(withMeta, "avif");
  assert.deepEqual(meta.exif, tiff);
});
