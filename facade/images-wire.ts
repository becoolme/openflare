// facade-images wire constants + shared helpers — inner `fetcher` of
// the `cloudflare-internal:images-api` wrapped binding. Speaks the
// upstream shim's own protocol: multipart/form-data POSTs to /info and
// /transform, errors signalled via the `cf-images-binding: err=<code>`
// response header (the shim reads that header and throws an
// ImagesError with the numeric code).

/** Images error surface per the upstream shim contract: non-2xx plus
 * `cf-images-binding: err=<code>`; the body text lands inside the
 * thrown `IMAGES_<OP> <body>` message. */
export function imagesError(code: number, message: string, status = 400): Response {
  return new Response(message, {
    status,
    headers: { "cf-images-binding": `err=${code}` },
  });
}

/** Hard input bound — CF documents ~70 MP source images; the byte cap
 * additionally guards the WASM engine's memory (a decompressed
 * 8-bit RGBA frame is ~4× pixels). */
export const IMAGES_BODY_MAX_BYTES = 64 * 1024 * 1024;
/** Pixel bound across decode/transform (CF ~70MP). */
export const IMAGES_MAX_PIXELS = 70_000_000;

/** Recognized container signatures → `image/*` format names on the
 * `info()` wire. */
export type ImageFormat = "jpeg" | "png" | "webp" | "gif" | "bmp" | "avif";

/** Parse container magic + dimensions without decoding pixels —
 * `info()` never runs the transform engine. Returns `null` on an
 * unrecognized/truncated buffer. */
export function sniffImage(buf: Uint8Array): { format: ImageFormat; width: number; height: number } | null {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  // PNG: 8-byte signature then IHDR (w,h big-endian u32).
  if (buf.length >= 24 && dv.getUint32(0) === 0x89504e47 && dv.getUint32(4) === 0x0d0a1a0a) {
    return { format: "png", width: dv.getUint32(16), height: dv.getUint32(20) };
  }
  // GIF: "GIF87a"/"GIF89a", w,h little-endian u16 at 6/8.
  if (buf.length >= 10 && dv.getUint32(0) === 0x47494638 && (buf[4] === 0x37 || buf[4] === 0x39)) {
    return { format: "gif", width: dv.getUint16(6, true), height: dv.getUint16(8, true) };
  }
  // BMP: "BM", w,h little-endian i32 at 18/22 (BITMAPINFOHEADER+).
  if (buf.length >= 26 && buf[0] === 0x42 && buf[1] === 0x4d) {
    return { format: "bmp", width: Math.abs(dv.getInt32(18, true)), height: Math.abs(dv.getInt32(22, true)) };
  }
  // AVIF/HEIC: ISO-BMFF `ftyp` box with an avif/avis brand.
  if (buf.length >= 12 && dv.getUint32(4) === 0x66747970 &&
      (dv.getUint32(8) === 0x61766966 || dv.getUint32(8) === 0x61766973)) {
    // Dims live in the meta box's `ispe` property: 4-byte size, 'ispe',
    // 4-byte version/flags, then u32 width/height. Scan the head — the
    // box sits early in every encoder's output.
    const scanEnd = Math.min(buf.length - 16, 65536);
    for (let off = 12; off < scanEnd; off++) {
      if (dv.getUint32(off + 4) === 0x69737065) {
        const w = dv.getUint32(off + 12);
        const h = dv.getUint32(off + 16);
        if (w > 0 && h > 0) return { format: "avif", width: w, height: h };
      }
    }
    return { format: "avif", width: 0, height: 0 };
  }
  // WebP: "RIFF".."WEBP" — VP8/VP8L/VP8X dimension forms.
  if (buf.length >= 30 && dv.getUint32(0) === 0x52494646 && dv.getUint32(8) === 0x57454250) {
    const fourcc = dv.getUint32(12);
    if (fourcc === 0x56503858) {
      // VP8X: 24-bit LE width-1/height-1 at 24/27.
      const w = (buf[24] | (buf[25] << 8) | (buf[26] << 16)) + 1;
      const h = (buf[27] | (buf[28] << 8) | (buf[29] << 16)) + 1;
      return { format: "webp", width: w, height: h };
    }
    if (fourcc === 0x56503820 && buf.length >= 30) {
      // VP8 lossy: frame tag at 20, dims (14-bit LE) at 26/28.
      return { format: "webp", width: dv.getUint16(26, true) & 0x3fff, height: dv.getUint16(28, true) & 0x3fff };
    }
    if (fourcc === 0x5650384c && buf.length >= 25) {
      // VP8L: 14-bit dims packed after the 0x2f signature at 21.
      const b = (buf[21] | (buf[22] << 8) | (buf[23] << 16) | (buf[24] << 24)) >>> 0;
      return { format: "webp", width: (b & 0x3fff) + 1, height: ((b >> 14) & 0x3fff) + 1 };
    }
    return { format: "webp", width: 0, height: 0 };
  }
  // JPEG: SOI + marker walk to the first SOFn carrying dims.
  if (buf.length >= 4 && dv.getUint16(0) === 0xffd8) {
    let off = 2;
    while (off + 9 < buf.length) {
      if (buf[off] !== 0xff) break;
      const marker = buf[off + 1];
      // SOF0-SOF15 except DHT/DAC/JPG/RSTn carry height+width.
      if (marker >= 0xc0 && marker <= 0xcf && marker !== 0xc4 && marker !== 0xc8 && marker !== 0xcc) {
        return { format: "jpeg", width: dv.getUint16(off + 7), height: dv.getUint16(off + 5) };
      }
      const len = dv.getUint16(off + 2);
      off += 2 + len;
    }
    return { format: "jpeg", width: 0, height: 0 };
  }
  return null;
}

/** SVG has no binary magic — the upstream mock and CF both report it
 * as a dim-less `{format}`-only info response; sniff the leading
 * markup instead. */
export function sniffSvg(buf: Uint8Array): boolean {
  const head = new TextDecoder().decode(buf.subarray(0, Math.min(buf.length, 4096)));
  const trimmed = head.trimStart();
  return trimmed.startsWith("<svg") || trimmed.startsWith("<?xml");
}
