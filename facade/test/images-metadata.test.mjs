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

// ---- real-world container shapes ----

/** Big-endian ("MM") TIFF: 3 IFD0 tags (offset + inline values) plus an
 * IFD1 pointer carrying a thumbnail tag — the camera-produced shape. */
function tiffMM() {
  const make = enc.encode("DEVCAM\0");      // 7 bytes → offset
  const cpr = enc.encode("(C) MM\0");       // 6 bytes → offset
  const xres = new Uint8Array(8);           // RATIONAL 72/1 → offset
  new DataView(xres.buffer).setUint32(0, 72); new DataView(xres.buffer).setUint32(4, 1);
  const ifd0n = 3;
  const dataOff = 8 + 2 + ifd0n * 12 + 4;
  const ifd1Off = dataOff + make.length + cpr.length + xres.length;
  const out = new Uint8Array(ifd1Off + 2 + 12 + 4);
  const dv = new DataView(out.buffer);
  out.set([0x4d, 0x4d]);                    // MM
  dv.setUint16(2, 42); dv.setUint32(4, 8);
  dv.setUint16(8, ifd0n);
  let d = dataOff;
  const entry = (i, tag, type, n, value) => {
    const e = 10 + i * 12;
    dv.setUint16(e, tag); dv.setUint16(e + 2, type); dv.setUint32(e + 4, n);
    if (n <= 4) { out.set(value, e + 8); }
    else { dv.setUint32(e + 8, d); out.set(value, d); d += value.length; }
  };
  entry(0, 0x010f, 2, make.length, make);   // Make
  entry(1, 0x8298, 2, cpr.length, cpr);     // Copyright
  entry(2, 0x011a, 5, 8, xres);             // XResolution RATIONAL
  dv.setUint32(10 + ifd0n * 12, ifd1Off);   // next IFD → IFD1
  dv.setUint16(ifd1Off, 1);
  dv.setUint16(ifd1Off + 2, 0x0201); dv.setUint16(ifd1Off + 4, 4);
  dv.setUint32(ifd1Off + 6, 1); dv.setUint32(ifd1Off + 10, 999);
  dv.setUint32(ifd1Off + 14, 0);
  return out;
}

test("copyright filter: big-endian multi-IFD TIFF keeps only 0x8298", () => {
  const mm = tiffMM();
  const src = jpegWithMeta(new Uint8Array([0xff, 0xd8, 0xff, 0xd9]), { exif: mm });
  const meta = extractMetadata(src, "jpeg");
  assert.deepEqual(meta.exif, mm, "keep passthrough is byte-identical");
  const filtered = attachMetadata(
    new Uint8Array([0xff, 0xd8, 0xff, 0xd9]), "jpeg",
    { exif: mm }, "copyright", 1, 1,
  );
  const app1 = jpegSegments(filtered, 0xe1);
  const seg = app1.find((s) => s[0] === 0x45);
  assert.ok(seg, "rebuilt EXIF present");
  const t = seg.subarray(6);
  const tdv = new DataView(t.buffer, t.byteOffset, t.byteLength);
  assert.equal(t[0], 0x49, "rebuilt as little-endian II");
  assert.equal(tdv.getUint16(8, true), 1, "exactly one tag");
  assert.equal(tdv.getUint16(10, true), 0x8298);
  const text = new TextDecoder().decode(t);
  assert.ok(text.includes("(C) MM"), "copyright text survives");
  assert.ok(!text.includes("DEVCAM"), "Make stripped");
});

/** 8BIM block: custom Pascal name + optional leading non-IPTC resources. */
function iptcBlockEx({ name = "", lead = [], datasets = [] }) {
  const blocks = [];
  for (const [id, payload] of [...lead, [0x0404, iimBytes(datasets)]]) {
    const nb = enc.encode(name);
    const nameTotal = 1 + nb.length + ((1 + nb.length) % 2);
    const blk = new Uint8Array(4 + 2 + nameTotal + 4 + payload.length + (payload.length & 1));
    const dv = new DataView(blk.buffer);
    blk.set(enc.encode("8BIM"), 0);
    dv.setUint16(4, id);
    blk[6] = nb.length;
    blk.set(nb, 7);
    dv.setUint32(6 + nameTotal, payload.length);
    blk.set(payload, 6 + nameTotal + 4);
    blocks.push(blk);
  }
  const total = blocks.reduce((s, b) => s + b.length, 0);
  const out = new Uint8Array(total);
  let p = 0;
  for (const b of blocks) { out.set(b, p); p += b.length; }
  return out;
}

/** IIM record-2 datasets; entry [ds, value, extendedLen?]. */
function iimBytes(datasets) {
  let len = 0;
  for (const [, v, ext] of datasets) len += (ext ? 7 : 5) + enc.encode(v).length;
  const out = new Uint8Array(len);
  const dv = new DataView(out.buffer);
  let q = 0;
  for (const [ds, v, ext] of datasets) {
    const vb = enc.encode(v);
    out[q] = 0x1c; out[q + 1] = 2; out[q + 2] = ds;
    if (ext) {
      dv.setUint32(q + 3, 0x80000000 | vb.length);
      out.set(vb, q + 7);
      q += 7 + vb.length;
    } else {
      dv.setUint16(q + 3, vb.length);
      out.set(vb, q + 5);
      q += 5 + vb.length;
    }
  }
  return out;
}

test("iptcToXmp: named 8BIM, leading non-IPTC resource, multi-value datasets, extended length", () => {
  const irb = iptcBlockEx({
    name: "mynrsrc",
    lead: [[0x03ed, new Uint8Array([1, 2, 3, 4])]], // resolution info, not IPTC
    datasets: [
      [0x74, "RIGHTS-1"],
      [0x19, "k1"], [0x19, "k2"],               // keywords repeat → rdf:Bag x2
      [0x78, "A".repeat(64), true],             // extended-length dataset
    ],
  });
  const xmp = new TextDecoder().decode(iptcToXmp(irb));
  assert.ok(xmp.includes("RIGHTS-1"), "copyright mapped");
  const bag = /dc:subject>.*?rdf:Bag(.*?)<\/rdf:Bag/s.exec(xmp);
  assert.ok(bag && bag[1].includes(">k1<") && bag[1].includes(">k2<"),
    "both keywords in rdf:Bag");
  assert.ok(xmp.includes("A".repeat(64)), "extended-length dataset decoded");
});

test("extractJpeg: COM and other APPn segments before metadata still parse", () => {
  const tiff = tiffWithCopyright();
  const segs = [
    jpegSeg(0xe0, new Uint8Array([0x4a, 0x46, 0x49, 0x46, 0])),  // JFIF APP0
    jpegSeg(0xfe, enc.encode("a comment")),                       // COM
    jpegSeg(0xe1, (() => { const p = new Uint8Array(6 + tiff.length); p.set(EXIF_PFX); p.set(tiff, 6); return p; })()),
  ];
  const out = new Uint8Array(2 + segs.reduce((s, x) => s + x.length, 0) + 2);
  out.set([0xff, 0xd8]); let p = 2;
  for (const s of segs) { out.set(s, p); p += s.length; }
  out.set([0xff, 0xd9], p);
  const meta = extractMetadata(out, "jpeg");
  assert.deepEqual(meta.exif, tiff);
});

test("extractMetadata: truncated segment yields partial meta, no throw", () => {
  const tiff = tiffWithCopyright();
  const full = jpegWithMeta(new Uint8Array([0xff, 0xd8, 0xff, 0xd9]),
    { exif: tiff, xmp: xmpPacket() });
  const cut = full.subarray(0, full.length - 5); // chop inside the XMP seg
  const meta = extractMetadata(cut, "jpeg");
  assert.deepEqual(meta.exif, tiff, "earlier EXIF still found");
  assert.equal(meta.xmp, undefined);
});

test("extractPng: compressed iTXt (flag=1) is skipped", () => {
  const kw = enc.encode("XML:com.adobe.xmp");
  const itxt = new Uint8Array(kw.length + 1 + 2 + 1 + 1 + 8);
  itxt.set(kw);
  itxt[kw.length + 1] = 1; // compressed — inflater unavailable, must skip
  const chunk = (t, d) => {
    const c = new Uint8Array(12 + d.length);
    const dv = new DataView(c.buffer);
    dv.setUint32(0, d.length); c.set(enc.encode(t), 4); c.set(d, 8);
    return c;
  };
  const sig = new Uint8Array(8);
  const dv8 = new DataView(sig.buffer);
  dv8.setUint32(0, 0x89504e47); dv8.setUint32(4, 0x0d0a1a0a);
  const png = new Uint8Array(8 + 25 + 12 + itxt.length + 12);
  let p = 0;
  png.set(sig, 0); p += 8;
  png.set(chunk("IHDR", new Uint8Array(13)), p); p += 25;
  png.set(chunk("iTXt", itxt), p); p += 12 + itxt.length;
  png.set(chunk("IEND", new Uint8Array(0)), p);
  const meta = extractMetadata(png, "png");
  assert.equal(meta.xmp, undefined);
});

test("extractMetadata: webp VP8X EXIF+XMP chunks", () => {
  const tiff = tiffWithCopyright();
  const xmp = xmpPacket();
  const riff = (fourcc, d) => {
    const c = new Uint8Array(8 + d.length + (d.length & 1));
    const dv = new DataView(c.buffer);
    c.set(enc.encode(fourcc)); dv.setUint32(4, d.length, true); c.set(d, 8);
    return c;
  };
  const vp8x = new Uint8Array(10);
  vp8x[0] = 0x0c; // exif|xmp flags
  const body = [riff("VP8X", vp8x), riff("EXIF", tiff), riff("XMP ", xmp)];
  const len = body.reduce((s, c) => s + c.length, 0);
  const out = new Uint8Array(12 + len);
  out.set(enc.encode("RIFF"));
  new DataView(out.buffer).setUint32(4, 4 + len, true);
  out.set(enc.encode("WEBP"), 8);
  let p = 12;
  for (const c of body) { out.set(c, p); p += c.length; }
  const meta = extractMetadata(out, "webp");
  assert.deepEqual(meta.exif, tiff);
  assert.deepEqual(meta.xmp, xmp);
});

test("transform: real PNG source with eXIf+iTXt → jpeg keep output", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "png", 90);
  const tiff = tiffWithCopyright();
  const xmp = xmpPacket();
  // Build the source PNG by attaching metadata to a real encoded PNG —
  // exercises the same writer against our own reader.
  const src = attachMetadata(base.bytes, "png", { exif: tiff, xmp }, "keep", 8, 8);
  const out = await transform(src, { metadata: "keep" });
  const app1 = jpegSegments(out, 0xe1);
  const exifSeg = app1.find((s) => s[0] === 0x45);
  const xmpSeg = app1.find((s) => s[0] === 0x68);
  assert.deepEqual(exifSeg?.subarray(6), tiff, "PNG eXIf → JPEG APP1");
  assert.ok(xmpSeg, "PNG iTXt xmp → JPEG APP1");
  // JFIF APP0 must remain first; our segments land after it.
  assert.equal(out[2], 0xff); assert.equal(out[3], 0xe0, "APP0 first");
  assert.equal(out[4 + new DataView(out.buffer, out.byteOffset).getUint16(4) + 1], 0xe1);
});

test("transform: real WebP source with EXIF+XMP → jpeg keep output", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "webp", 90);
  const tiff = tiffWithCopyright();
  const xmp = xmpPacket();
  const src = attachMetadata(base.bytes, "webp", { exif: tiff, xmp }, "keep", 8, 8);
  const out = await transform(src, { metadata: "keep" });
  const app1 = jpegSegments(out, 0xe1);
  const exifSeg = app1.find((s) => s[0] === 0x45);
  const xmpSeg = app1.find((s) => s[0] === 0x68);
  assert.deepEqual(exifSeg?.subarray(6), tiff, "webp EXIF → jpeg APP1");
  assert.ok(xmpSeg, "webp XMP → jpeg APP1");
});

test("transform: APP13 with only non-IPTC Photoshop resources → no XMP mapping on png out", async () => {
  const eng = await import(facadeModule("images-engine.js"));
  const base = await eng.encodeImage(solidFrame(8, 8), "jpeg", 90);
  const irb = iptcBlockEx({
    name: "res",
    lead: [], datasets: [], // will be ignored
  });
  // Build manually: single 0x03ed resource, no 0x0404.
  const dv0 = new Uint8Array(4 + 2 + 2 + 4 + 4);
  dv0.set(enc.encode("8BIM"), 0);
  new DataView(dv0.buffer).setUint16(4, 0x03ed);
  new DataView(dv0.buffer).setUint32(8, 4);
  dv0.set([9, 9, 9, 9], 12);
  const src = jpegWithMeta(base.bytes, { iptc: dv0 });
  const out = await transform(src, {
    metadata: "keep",
    output_format: "image/png",
  });
  assert.equal(pngChunks(out, "iTXt").length, 0,
    "non-0404 Photoshop block produces no XMP");
});

test("transform: gif source + metadata=keep still transforms, no metadata attached", async () => {
  const realGif = Uint8Array.from(
    atob("R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw=="),
    (c) => c.charCodeAt(0),
  );
  const out = await transform(realGif, {
    metadata: "keep",
    output_format: "image/png",
  });
  assert.equal(pngChunks(out, "eXIf").length, 0);
  assert.equal(pngChunks(out, "iTXt").length, 0);
});

test("copyright filter: inline (<=4 byte) copyright value survives", () => {
  const tiff = new Uint8Array(8 + 2 + 12 + 4);
  const dv = new DataView(tiff.buffer);
  tiff.set([0x49, 0x49]); dv.setUint16(2, 42, true); dv.setUint32(4, 8, true);
  dv.setUint16(8, 1);
  dv.setUint16(10, 0x8298, true); dv.setUint16(12, 2, true);
  dv.setUint32(14, 3, true);                  // 3 bytes → inline
  tiff.set(enc.encode("C)\0"), 18);
  const out = attachMetadata(
    new Uint8Array([0xff, 0xd8, 0xff, 0xd9]), "jpeg",
    { exif: tiff }, "copyright", 1, 1,
  );
  const seg = jpegSegments(out, 0xe1).find((s) => s[0] === 0x45);
  assert.ok(seg, "inline copyright kept");
  const t = seg.subarray(6);
  const tdv = new DataView(t.buffer, t.byteOffset, t.byteLength);
  assert.equal(tdv.getUint32(14, true), 3, "inline count preserved");
});
