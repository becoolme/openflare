// facade-images system worker — inner `fetcher` of the
// `cloudflare-internal:images-api` wrapped binding. Speaks the
// UPSTREAM shim's wire contract (see workerd
// src/cloudflare/internal/images-api.ts): multipart/form-data POSTs,
// the image bytes ride the `image` file field, the op chain rides the
// `transforms` JSON field, errors are signalled by a
// `cf-images-binding: err=<code>` response header.
//
// `info()` parses container headers in pure TS (cheap, no decode).
// `transform()` runs the vendored jSquash WASM engine
// (facade/images-engine.ts) — decode → op chain → encode entirely
// inside this worker. `text_input`/`draw_text` rasterization, animated
// formats, GIF/BMP decode, AVIF/GIF encode, and `segment` gravity fail
// closed — never a silent identity passthrough.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  imagesError,
  sniffImage,
  sniffSvg,
  IMAGES_BODY_MAX_BYTES,
  IMAGES_MAX_PIXELS,
} from "./images-wire.js";
import {
  EngineError,
  decodeImage,
  encodeImage,
  parseColor,
  composite,
  type ImageDataLike,
} from "./images-engine.js";
import {
  applyTransforms,
  IMAGES_TRANSFORM_MAX_PIXELS,
} from "./images-transform.js";

const DECODE_FORMATS = new Set(["jpeg", "png", "webp", "avif"]);
const OUTPUT_FORMATS: Record<string, string> = {
  "image/jpeg": "jpeg",
  "image/png": "png",
  "image/webp": "webp",
  rgb: "rgb",
  rgba: "rgba",
};

function wireError(e: unknown): Response {
  if (e instanceof EngineError) {
    return imagesError(e.code, e.message, e.code === 9423 ? 400 : 415);
  }
  return imagesError(9523, "images: internal transform error", 500);
}

/** Decode a multipart file field into an RGBA frame, or throw a typed
 * EngineError. */
async function decodeField(file: File, what: string): Promise<ImageDataLike> {
  const bytes = new Uint8Array(await file.arrayBuffer());
  if (bytes.length === 0 || bytes.length > IMAGES_BODY_MAX_BYTES) {
    throw new EngineError(9409, `images: ${what} empty or over the 64MiB cap`);
  }
  if (sniffSvg(bytes)) {
    throw new EngineError(9401, `images: ${what} is SVG — rasterization is not supported`);
  }
  const sniffed = sniffImage(bytes);
  if (!sniffed) {
    throw new EngineError(9420, `images: ${what} is not a recognized image`);
  }
  if (!DECODE_FORMATS.has(sniffed.format)) {
    throw new EngineError(9401, `images: ${what} format ${sniffed.format} has no vendored decoder`);
  }
  return decodeImage(bytes, sniffed.format);
}

/** Alpha-flatten onto `color` for opaque output formats. */
function flatten(img: ImageDataLike, color: [number, number, number, number]): ImageDataLike {
  const base: ImageDataLike = {
    data: new Uint8ClampedArray(img.width * img.height * 4),
    width: img.width,
    height: img.height,
  };
  for (let i = 0; i < base.data.length; i += 4) {
    base.data[i] = color[0];
    base.data[i + 1] = color[1];
    base.data[i + 2] = color[2];
    base.data[i + 3] = color[3];
  }
  composite(base, img, 0, 0, 1, "over");
  return base;
}

export class ImagesFacade extends WorkerEntrypoint {
  override async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (req.method !== "POST") {
      return imagesError(9406, "images: method not allowed", 405);
    }
    let form: FormData;
    try {
      form = await req.formData();
    } catch {
      return imagesError(9423, "images: malformed multipart body", 400);
    }
    switch (url.pathname) {
      case "/info": {
        const image = form.get("image");
        if (!(image instanceof File)) {
          return imagesError(9423, "images: missing `image` field", 400);
        }
        const body = new Uint8Array(await image.arrayBuffer());
        if (body.length === 0 || body.length > IMAGES_BODY_MAX_BYTES) {
          return imagesError(9409, "images: body empty or over the 64MiB cap", 413);
        }
        if (sniffSvg(body)) {
          // Dim-less shape, matching CF's SVG info() response.
          return Response.json({ format: "image/svg+xml" });
        }
        const sniffed = sniffImage(body);
        if (!sniffed) {
          return imagesError(9420, "images: unsupported or corrupt image data", 415);
        }
        if (sniffed.width * sniffed.height > IMAGES_MAX_PIXELS) {
          return imagesError(9401, "images: source exceeds the 70MP cap", 413);
        }
        // Upstream wire shape is snake_case; the shim maps it to
        // {fileSize, width, height, format}.
        return Response.json({
          format: `image/${sniffed.format}`,
          file_size: body.length,
          width: sniffed.width,
          height: sniffed.height,
        });
      }
      case "/transform": {
        try {
          // Text-source canvases need a font rasterizer — none vendored.
          if (form.get("text_input") !== null) {
            return imagesError(9401, "images: text_input rasterization is not supported", 415);
          }
          if (form.get("draw_text") !== null) {
            return imagesError(9401, "images: draw_text rasterization is not supported", 415);
          }
          const image = form.get("image");
          if (!(image instanceof File)) {
            return imagesError(9423, "images: missing `image` field", 400);
          }
          const raw = form.get("transforms");
          if (typeof raw !== "string") {
            return imagesError(9423, "images: missing `transforms` field", 400);
          }
          let ops: unknown[];
          try {
            const parsed: unknown = JSON.parse(raw);
            if (!Array.isArray(parsed)) throw new Error("shape");
            ops = parsed;
          } catch {
            return imagesError(9423, "images: malformed `transforms` JSON", 400);
          }

          const canvas = await decodeField(image, "canvas image");
          const frames = [canvas];
          for (const field of form.getAll("draw_image")) {
            if (!(field instanceof File)) {
              return imagesError(9423, "images: malformed `draw_image` field", 400);
            }
            frames.push(await decodeField(field, `draw_image ${frames.length}`));
          }
          if (canvas.width * canvas.height > IMAGES_TRANSFORM_MAX_PIXELS) {
            return imagesError(9401, "images: source exceeds the transform pixel cap", 413);
          }

          const output = form.get("output_format");
          if (typeof output !== "string" || !output) {
            return imagesError(9423, "images: missing `output_format`", 400);
          }
          const format = OUTPUT_FORMATS[output];
          if (!format) {
            return imagesError(9401, `images: ${output} output is not supported`, 415);
          }
          const rawQuality = form.get("output_quality");
          const quality = rawQuality === null ? 85 : Number(rawQuality);
          if (!Number.isFinite(quality) || quality < 1 || quality > 100) {
            return imagesError(9423, "images: invalid `output_quality`", 400);
          }
          // `anim` only matters for animated sources, which the vendored
          // engine never produces (no GIF decode) — accepted and ignored,
          // matching the "single frame" output semantics CF gives a
          // still image.
          const rawBg = form.get("background");
          const bg = rawBg === null
            ? null
            : typeof rawBg === "string"
              ? parseColor(rawBg)
              : null;
          if (rawBg !== null && !bg) {
            return imagesError(9423, "images: invalid `background` color", 400);
          }

          const done = await applyTransforms(frames, ops);
          const needsFlatten = bg !== null || format === "jpeg";
          const finalImage = needsFlatten
            ? flatten(done, bg ?? [255, 255, 255, 255])
            : done;
          const { bytes, contentType } = await encodeImage(finalImage, format, quality);
          return new Response(bytes, {
            status: 200,
            headers: { "content-type": contentType },
          });
        } catch (e) {
          return wireError(e);
        }
      }
      default:
        return imagesError(9404, "images: unknown op", 404);
    }
  }
}
