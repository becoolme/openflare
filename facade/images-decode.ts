// facade-images pure-TS decoders for formats jSquash does not ship:
// GIF (first frame — the `anim` surface stays fail-closed upstream)
// and BMP (uncompressed 24/32bpp). Both produce the engine's
// ImageDataLike frame; anything outside the covered subset throws
// EngineError so the wire layer maps it to `err=<code>`.
import { EngineError, type ImageDataLike } from "./images-engine.js";

function u16(b: Uint8Array, o: number): number {
  return b[o] | (b[o + 1] << 8);
}
function u32(b: Uint8Array, o: number): number {
  return (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;
}

// ---------------------------------------------------------------- GIF
/** GIF LZW decompression — variable-width codes 3..12 bits, little-
 * endian bit packing, clear/EOI control codes. Dict entries are
 * {prefix code, suffix byte} chains expanded by walking back to a
 * literal; memory is O(4096) regardless of output size. */
function lzwDecode(bytes: Uint8Array, minCodeSize: number, out: Uint8Array): void {
  const clear = 1 << minCodeSize;
  const eoi = clear + 1;
  // dict[i] for i >= eoi+1: {prefix, byte}; literals i < clear decode as themselves.
  const prefix = new Int32Array(4096);
  const suffix = new Uint8Array(4096);
  // Stack for chain expansion (deepest chain = 4096).
  const stack = new Uint8Array(4096);
  let codeSize = minCodeSize + 1;
  let next = eoi + 1;
  let bitPos = 0;
  let prev = -1;
  let outPos = 0;
  const reset = () => {
    codeSize = minCodeSize + 1;
    next = eoi + 1;
    prev = -1;
  };
  const readCode = (): number => {
    let code = 0;
    for (let i = 0; i < codeSize; i++) {
      const byte = bytes[bitPos >> 3];
      if (byte === undefined) return -1;
      code |= ((byte >> (bitPos & 7)) & 1) << i;
      bitPos++;
    }
    return code;
  };
  const bad = () => {
    throw new EngineError(9420, "images: gif lzw bad code");
  };
  for (;;) {
    const code = readCode();
    if (code < 0 || code === eoi) break;
    if (code === clear) {
      reset();
      continue;
    }
    if (prev === -1) {
      // First code after a clear must be a literal.
      if (code >= clear) bad();
      if (outPos < out.length) out[outPos++] = code;
      prev = code;
      continue;
    }
    // Expand `code` into the stack (reverse order), resolving KwKwK.
    let sp = 0;
    let expand = code;
    let firstByte = -1;
    if (expand === next && next < 4096) {
      stack[sp++] = firstByteOf(prefix, suffix, clear, prev);
      expand = prev;
    } else if (expand >= next) {
      bad();
    }
    while (expand >= clear) {
      if (sp >= 4096) bad();
      stack[sp++] = suffix[expand];
      expand = prefix[expand];
    }
    if (expand < 0 || expand >= clear) bad();
    firstByte = expand;
    stack[sp++] = expand;
    // Emit (stack holds reversed expansion).
    while (sp > 0) {
      if (outPos < out.length) out[outPos++] = stack[--sp];
      else sp = 0;
    }
    // New dict entry: prev-chain + first byte of this expansion.
    if (next < 4096) {
      prefix[next] = prev;
      suffix[next] = firstByte;
      next++;
      // GIF spec: bump code size when next crosses the current width.
      if (next === 1 << codeSize && codeSize < 12) codeSize++;
    }
    prev = code;
  }
}

/** First byte of the expansion of `code` (walks the chain to its
 * terminal literal). */
function firstByteOf(prefix: Int32Array, suffix: Uint8Array, clear: number, code: number): number {
  void suffix;
  let c = code;
  while (c >= clear) c = prefix[c];
  return c;
}



/** Sub-block collector — GIF data arrives in ≤255-byte chunks. */
function readSubBlocks(b: Uint8Array, o: number): { data: Uint8Array; next: number } {
  const chunks: number[] = [];
  let pos = o;
  for (;;) {
    const n = b[pos];
    if (n === undefined || n === 0) return { data: new Uint8Array(chunks), next: pos + 1 };
    for (let i = 0; i < n; i++) {
      if (b[pos + 1 + i] === undefined) throw new EngineError(9420, "images: gif truncated sub-block");
      chunks.push(b[pos + 1 + i]);
    }
    pos += 1 + n;
  }
}

/** Decode a GIF's FIRST frame to RGBA. Later frames are ignored —
 * multi-frame `anim` handling is rejected upstream of decode. */
export function decodeGif(bytes: Uint8Array): ImageDataLike {
  if (bytes.length < 13 || (bytes[0] !== 0x47 || bytes[1] !== 0x49 || bytes[2] !== 0x46)) {
    throw new EngineError(9420, "images: not a gif");
  }
  const width = u16(bytes, 6);
  const height = u16(bytes, 8);
  if (!width || !height || width > 16384 || height > 16384) {
    throw new EngineError(9420, "images: gif bad dimensions");
  }
  const packed = bytes[10];
  const gctSize = packed & 0x80 ? 1 << ((packed & 7) + 1) : 0;
  let pos = 13;
  const gct = new Uint8Array(gctSize * 3);
  if (gctSize) {
    if (pos + gct.length > bytes.length) throw new EngineError(9420, "images: gif truncated gct");
    gct.set(bytes.subarray(pos, pos + gct.length));
    pos += gct.length;
  }

  // GCE state carried into the first image descriptor.
  let transparent = -1;
  let imgData: { lct: Uint8Array | null; left: number; top: number; w: number; h: number; interlace: boolean; minCode: number; data: Uint8Array } | null = null;
  while (pos < bytes.length && imgData === null) {
    const tag = bytes[pos++];
    if (tag === 0x3b) break; // trailer
    if (tag === 0x21) {
      const label = bytes[pos++];
      if (label === 0xf9) {
        const size = bytes[pos++];
        if (size !== 4) throw new EngineError(9420, "images: gif bad gce");
        const p = bytes[pos];
        transparent = p & 1 ? bytes[pos + 3] : -1;
        pos += size;
        if (bytes[pos] !== 0) throw new EngineError(9420, "images: gif gce missing terminator");
        pos++;
      } else {
        const { next } = readSubBlocks(bytes, pos);
        pos = next;
      }
      continue;
    }
    if (tag !== 0x2c) break; // unknown block — stop, fail on no image
    if (pos + 9 > bytes.length) throw new EngineError(9420, "images: gif truncated descriptor");
    const left = u16(bytes, pos);
    const top = u16(bytes, pos + 2);
    const w = u16(bytes, pos + 4);
    const h = u16(bytes, pos + 6);
    const ip = bytes[pos + 8];
    pos += 9;
    if (!w || !h || left + w > width || top + h > height) {
      throw new EngineError(9420, "images: gif bad frame rect");
    }
    const lctSize = ip & 0x80 ? 1 << ((ip & 7) + 1) : 0;
    let lct: Uint8Array | null = null;
    if (lctSize) {
      lct = new Uint8Array(lctSize * 3);
      if (pos + lct.length > bytes.length) throw new EngineError(9420, "images: gif truncated lct");
      lct.set(bytes.subarray(pos, pos + lct.length));
      pos += lct.length;
    }
    if (pos >= bytes.length) throw new EngineError(9420, "images: gif missing data");
    const minCode = bytes[pos++];
    if (minCode < 2 || minCode > 8) throw new EngineError(9420, "images: gif bad lzw code size");
    const { data, next } = readSubBlocks(bytes, pos);
    pos = next;
    imgData = { lct, left, top, w, h, interlace: (ip & 0x40) !== 0, minCode, data };
  }
  if (!imgData) throw new EngineError(9420, "images: gif has no image frame");

  const indices = new Uint8Array(imgData.w * imgData.h);
  lzwDecode(imgData.data, imgData.minCode, indices);
  if (imgData.interlace) {
    const src = indices.slice();
    const order: number[] = [];
    for (const [start, step] of [[0, 8], [4, 8], [2, 4], [1, 2]] as const) {
      for (let y = start; y < imgData.h; y += step) order.push(y);
    }
    let s = 0;
    for (const y of order) {
      indices.set(src.subarray(s, s + imgData.w), y * imgData.w);
      s += imgData.w;
    }
  }

  const pal = imgData.lct ?? (gctSize ? gct : null);
  if (!pal) throw new EngineError(9420, "images: gif frame has no palette");
  // Canvas: transparent everywhere; the frame rect is stamped in.
  const out = new Uint8ClampedArray(width * height * 4);
  for (let y = 0; y < imgData.h; y++) {
    for (let x = 0; x < imgData.w; x++) {
      const idx = indices[y * imgData.w + x];
      if (idx === transparent) continue;
      const p = idx * 3;
      if (p + 2 >= pal.length) continue;
      const o = ((imgData.top + y) * width + imgData.left + x) * 4;
      out[o] = pal[p];
      out[o + 1] = pal[p + 1];
      out[o + 2] = pal[p + 2];
      out[o + 3] = 255;
    }
  }
  return { data: out, width, height };
}

// ---------------------------------------------------------------- BMP
/** Decode an uncompressed Windows BMP (24bpp BGR / 32bpp BGRA,
 * bottom-up or top-down). RLE and palette modes fail closed — CF's
 * input surface never promised BMP anyway (see docs). */
export function decodeBmp(bytes: Uint8Array): ImageDataLike {
  if (bytes.length < 54 || bytes[0] !== 0x42 || bytes[1] !== 0x4d) {
    throw new EngineError(9420, "images: not a bmp");
  }
  const dataOff = u32(bytes, 10);
  const dibSize = u32(bytes, 14);
  if (dibSize < 40) throw new EngineError(9401, "images: bmp variant unsupported");
  const w = u32(bytes, 18) | 0;
  const hRaw = u32(bytes, 22) | 0;
  const topDown = hRaw < 0;
  const h = Math.abs(hRaw);
  const planes = u16(bytes, 26);
  const bpp = u16(bytes, 28);
  const compression = u32(bytes, 30);
  if (planes !== 1 || (bpp !== 24 && bpp !== 32) || compression !== 0) {
    throw new EngineError(9401, `images: bmp ${bpp}bpp/compression=${compression} unsupported`);
  }
  if (!w || !h || w > 16384 || h > 16384 || w * h > 70_000_000) {
    throw new EngineError(9420, "images: bmp bad dimensions");
  }
  const stride = Math.ceil((w * bpp) / 32) * 4;
  if (dataOff + stride * h > bytes.length) {
    throw new EngineError(9420, "images: bmp truncated");
  }
  const out = new Uint8ClampedArray(w * h * 4);
  for (let y = 0; y < h; y++) {
    const sy = topDown ? y : h - 1 - y;
    const row = dataOff + sy * stride;
    for (let x = 0; x < w; x++) {
      const s = row + x * (bpp / 8);
      const d = (y * w + x) * 4;
      out[d] = bytes[s + 2];
      out[d + 1] = bytes[s + 1];
      out[d + 2] = bytes[s];
      out[d + 3] = bpp === 32 ? bytes[s + 3] : 255;
    }
  }
  return { data: out, width: w, height: h };
}
