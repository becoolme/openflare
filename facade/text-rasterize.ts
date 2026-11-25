// Text → RGBA rasterization for `text_input` / `draw_text` wire
// fields, backed by vendored `@resvg/resvg-wasm`: fontdb parses
// ttf/otf/woff/woff2 from the supplied bytes (no system fonts, no
// bundled font assets — fonts arrive over the wire like overlays),
// rustybuzz shapes, tiny-skia rasterizes. Spec validation lives in
// text-spec.ts so the egress worker can check options without
// loading this module's wasm.

import { initWasm, Resvg } from "../third_party/resvg/index.js";
import resvgWasm from "../third_party/resvg/index_bg.wasm";
import { EngineError, type ImageDataLike } from "./images-engine.js";
import {
  FONT_MAX_BYTES,
  TEXT_MAX_DIM,
  looksLikeFont,
  type TextSpec,
} from "./text-spec.js";

let resvgReady: Promise<void> | undefined;
function ensureResvg(): Promise<void> {
  return (resvgReady ??= initWasm(
    resvgWasm as unknown as WebAssembly.Module,
  ));
}

const escapeXml = (s: string): string =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/** Rasterize `spec` with `fontBytes` into a straight-alpha RGBA
 * frame sized to the text's tight bounding box. Whitespace-only text
 * yields a 1×1 transparent frame (the document renders nothing).
 * resvg pixmaps are premultiplied — converted here. */
export async function rasterizeText(
  spec: TextSpec,
  fontBytes: Uint8Array,
): Promise<ImageDataLike> {
  if (fontBytes.length === 0 || fontBytes.length > FONT_MAX_BYTES) {
    throw new EngineError(9420, "images: font empty or over the 20MiB cap");
  }
  if (!looksLikeFont(fontBytes)) {
    throw new EngineError(9420, "images: font not a recognized font file");
  }
  await ensureResvg();
  // Generous canvas so ascender/descender overhangs never clip; the
  // bbox crop pulls the frame back down to the text's true extents.
  const cw = Math.min(TEXT_MAX_DIM, Math.ceil(spec.size * (spec.text.length + 2)));
  const ch = Math.min(TEXT_MAX_DIM, Math.ceil(spec.size * 3));
  const pad = Math.ceil(spec.size);
  const svg =
    `<svg xmlns="http://www.w3.org/2000/svg" width="${cw}" height="${ch}">` +
    `<text x="${pad}" y="${pad + spec.size}" font-size="${spec.size}" fill="${spec.color}">` +
    escapeXml(spec.text) +
    "</text></svg>";
  let resvg;
  try {
    resvg = new Resvg(svg, {
      font: { fontBuffers: [fontBytes], loadSystemFonts: false },
    });
  } catch (e) {
    throw new EngineError(9420, `images: font could not be parsed: ${e}`);
  }
  const bbox = resvg.innerBBox();
  if (bbox === undefined || bbox.width < 1 || bbox.height < 1) {
    // Only whitespace rendered — or every glyph missing.
    return {
      data: new Uint8ClampedArray(4),
      width: 1,
      height: 1,
    };
  }
  // A bbox saturating the capped canvas means the render clipped —
  // the true result exceeds upstream's 4096² bound either way.
  if (bbox.width >= TEXT_MAX_DIM || bbox.height >= TEXT_MAX_DIM) {
    throw new EngineError(9423, "images: rendered text exceeds the 4096px bound");
  }
  let img;
  try {
    resvg.cropByBBox(bbox);
    img = resvg.render();
  } catch (e) {
    throw new EngineError(9420, `images: text rasterization failed: ${e}`);
  }
  const src = img.pixels;
  const data = new Uint8ClampedArray(src.length);
  for (let i = 0; i < src.length; i += 4) {
    const a = src[i + 3];
    if (a === 0) continue;
    if (a === 255) {
      data[i] = src[i];
      data[i + 1] = src[i + 1];
      data[i + 2] = src[i + 2];
    } else {
      // premultiplied → straight
      data[i] = Math.min(255, Math.round((src[i] * 255) / a));
      data[i + 1] = Math.min(255, Math.round((src[i + 1] * 255) / a));
      data[i + 2] = Math.min(255, Math.round((src[i + 2] * 255) / a));
    }
    data[i + 3] = a;
  }
  return { data, width: img.width, height: img.height };
}
