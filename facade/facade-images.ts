// facade-images system worker — inner `fetcher` of the
// `cloudflare-internal:images-api` wrapped binding. Speaks the
// UPSTREAM shim's wire contract (see workerd
// src/cloudflare/internal/images-api.ts): multipart/form-data POSTs,
// the image bytes ride the `image` file field, errors are signalled
// by a `cf-images-binding: err=<code>` response header.
//
// `info()` parses container headers in pure TS (cheap, no decode).
// `transform()` validates the upstream `transforms` field then fails
// closed with a typed error until the jSquash WASM engine is vendored
// — never a silent source passthrough.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  imagesError,
  sniffImage,
  sniffSvg,
  IMAGES_BODY_MAX_BYTES,
  IMAGES_MAX_PIXELS,
} from "./images-wire.js";

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
    const image = form.get("image");
    if (!(image instanceof File)) {
      return imagesError(9423, "images: missing `image` field", 400);
    }
    const body = new Uint8Array(await image.arrayBuffer());
    if (body.length === 0 || body.length > IMAGES_BODY_MAX_BYTES) {
      return imagesError(9409, "images: body empty or over the 64MiB cap", 413);
    }
    switch (url.pathname) {
      case "/info": {
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
        const raw = form.get("transforms");
        if (typeof raw !== "string") {
          return imagesError(9423, "images: missing `transforms` field", 400);
        }
        try {
          const parsed: unknown = JSON.parse(raw);
          if (!Array.isArray(parsed)) {
            return imagesError(9423, "images: `transforms` must be a JSON array", 400);
          }
        } catch {
          return imagesError(9423, "images: malformed `transforms` JSON", 400);
        }
        return imagesError(
          9490,
          "images: transform engine not vendored in this build",
          501,
        );
      }
      default:
        return imagesError(9404, "images: unknown op", 404);
    }
  }
}
