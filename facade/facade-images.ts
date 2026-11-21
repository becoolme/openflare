// facade-images system worker — inner `fetcher` of the
// `cloudflare-internal:images-api` wrapped binding. Runs the CF
// Images binding surface IN-worker: no provider hop, no secrets, the
// request body is the raw image bytes and params ride one header.
//
// Engine: `info()` parses container headers in pure TS (cheap, no
// decode). `transform()` routes to the vendored jSquash WASM engine
// (jpeg/png/webp codecs + lanczos3 resize) — vendoring lands with the
// engine module; until then the op fails closed with a typed error
// rather than a silent passthrough.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  imagesError,
  sniffImage,
  IMAGES_BODY_MAX_BYTES,
  IMAGES_MAX_PIXELS,
  IMAGES_PARAMS_HEADER,
} from "./images-wire.js";

export class ImagesFacade extends WorkerEntrypoint {
  override async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (req.method !== "POST") {
      return imagesError("9406", "images: method not allowed", 405);
    }
    const body = new Uint8Array(await req.arrayBuffer());
    if (body.length === 0 || body.length > IMAGES_BODY_MAX_BYTES) {
      return imagesError("9409", "images: body empty or over the 64MiB cap", 413);
    }
    switch (url.pathname) {
      case "/info": {
        const sniffed = sniffImage(body);
        if (!sniffed) {
          return imagesError("9420", "images: unsupported or corrupt image data", 415);
        }
        if (sniffed.width * sniffed.height > IMAGES_MAX_PIXELS) {
          return imagesError("9401", "images: source exceeds the 70MP cap", 413);
        }
        return Response.json({
          fileSize: body.length,
          format: `image/${sniffed.format}`,
          width: sniffed.width,
          height: sniffed.height,
        });
      }
      case "/transform": {
        let params: unknown;
        try {
          params = JSON.parse(req.headers.get(IMAGES_PARAMS_HEADER) ?? "{}");
        } catch {
          return imagesError("9423", "images: malformed transform params", 400);
        }
        void params;
        return imagesError(
          "9490",
          "images: transform engine not vendored in this build",
          501,
        );
      }
      default:
        return imagesError("9404", "images: unknown op", 404);
    }
  }
}
