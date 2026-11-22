# Images binding

`env.IMAGES` is the Workers Images binding: `info(stream)` reports
container metadata and `input(stream).transform(opts).output(opts)`
runs an image-transformation pipeline inside the platform.

**Status: Partial** — `info()` and `transform()` are implemented with
a vendored jSquash WASM engine. Coverage and fidelity limits are
listed under *Known differences*; everything the engine cannot do
correctly fails closed with a typed error rather than silently
passing the source bytes through.

## Wrangler configuration

```toml
[images]
binding = "IMAGES"
```

Native `vrt.toml` equivalent:

```toml
[bindings]
images = ["IMAGES"]
```

`binding` is the only accepted key besides the optional boolean
`remote`, which is accepted and ignored — matching the local-dev
precedent used by `kv`/`k2`/`analytics_engine_datasets` (Cloudflare
uses `remote` to opt a binding into remote mode for `wrangler dev`;
the VRT runtime is always local). Anything else fails closed:
`VRT_CONFIG_UNSUPPORTED_FIELD` (`images.<key>`), and a missing or
non-string `binding` is `VRT_CONFIG_INVALID` (`images.binding`).

## Using it

```js
export default {
  async fetch(req, env) {
    const body = (await fetch("https://example.com/cat.png")).body;

    const info = await env.IMAGES.info(body);
    // → { fileSize, format: "image/png", width, height }

    const out = await env.IMAGES
      .input(body)
      .transform({ width: 200, fit: "cover" })
      .transform({ rotate: 90 })
      .output({ format: "image/webp", quality: 85 });
    return out.response();
  },
};
```

## Where it runs

The binding renders as a workerd `wrapped` binding over
`cloudflare-internal:images-api` — the upstream workerd shim, shipped
unmodified in the pinned fork — whose inner `fetcher` service binding
points at the `facade-images` system worker (`vrt-config/src/capnp.rs`,
`facade/facade-images.ts`). The facade is deny-routed (no network
egress): it only receives the image bytes the tenant worker streams
in.

The facade speaks the shim's own upstream protocol verbatim —
multipart/form-data POSTs to `/info` and `/transform` (the `image`
file field carries the bytes, `transforms` a JSON op array, extra
`draw_image` file fields carry overlay sources), and errors ride the
`cf-images-binding: err=<code>` response header that the shim
translates into a thrown `ImagesError`. No shim fork, no parallel
protocol.

## The engine

`facade/images-engine.ts` decodes → transforms → encodes entirely
inside the system worker using vendored
[jSquash](https://github.com/jamsinclair/jSquash) WASM modules
(`facade/third_party/jsquash/`): mozjpeg, Squoosh PNG, WebP
(lossy+lossless), AVIF **decode only**, and `@jsquash/resize`
(Lanczos3/ triangle/bilinear/nearest). `.wasm` modules travel the same
manifest → capnp `wasm = embed` pipeline as ES modules
(`facade/tools/gen-manifest.mjs`, `vrt-config/src/capnp.rs`,
`vrt-config/src/sysmanifest.rs`); Node tests load them through the
same `WebAssembly.Module` injection path workerd uses
(`facade/test/shims/resolve-hooks.mjs`).

Transform ops execute in this order inside `images-transform.ts`:
trim → gravity crop → fit resize/pad → rotate → flip → color ops
(contrast/brightness/saturation/hue-rotate/invert/sepia/gamma/
grayscale/opacity) → blur → sharpen → border paint. `draw()` ops
composite overlays with the standard alpha-compositing modes
(`source-over`, `multiply`, `screen`, `overlay`, `lighter`/additive,
and the rest of the upstream op set).

## How the ops map

| op | implementation |
| --- | --- |
| `info(stream)` | Pure-TS container sniffer (`facade/images-wire.ts`) — reads PNG IHDR, JPEG SOFn, WebP VP8/VP8L/VP8X, GIF and BMP headers, ISO-BMFF `ftyp`+`ispe` for AVIF/HEIC; SVG markup returns the dim-less `{format: "image/svg+xml"}` shape. Returns the wire `{file_size, format, width, height}` the shim maps to `fileSize`/`width`/`height`/`format`; the pixel engine never runs. |
| `input().transform()/.draw().output()` | The upstream shim serializes the op chain into the `transforms` multipart field (plus `output_format`/`output_quality`/`background`/`anim`/`text_input`/`draw_*`). The facade validates, decodes every `image`/`draw_image` file, applies the ops, flattens alpha over `background` when required, and encodes. `input()`'s `encoding: "base64"` decode and `text()` sources are handled inside the shim already. |
| `hosted.*` (`env.IMAGES.hosted.image()/.upload()/.list()/.createDirectUpload()`) | Cloudflare Images **product** RPC surface — not implemented; the facade exposes no such methods so calls reject. |

## Backends

None. `facade-images` transforms only the bytes streamed to it — the
WASM engine is vendored into the system worker itself, so no provider
socket, credentials or stored state is wired (the facade is
deny-routed like every other system worker). `[images]` accepts no
resource-identifying keys besides `binding`.

## Unsupported keys and error shape

- `[images]` accepts `binding` and `remote` only — anything else →
  `VRT_CONFIG_UNSUPPORTED_FIELD` (`images.<key>`).
- Missing/non-string `binding` → `VRT_CONFIG_INVALID`
  (`images.binding`); non-table `images` or non-boolean `remote` →
  `VRT_CONFIG_INVALID` (`images`, `images.remote`).
- A binding name colliding with another binding on the same worker →
  `DESIRED_DUPLICATE` (`bindingName`) at snapshot validation.
- At runtime the facade answers non-2xx with the
  `cf-images-binding: err=<code>` header; the upstream
  `cloudflare-internal:images-api` shim turns it into a thrown
  `ImagesError` (`IMAGES_<OP> <body>`) carrying numeric `.code`.

## Bounds

- Input body ≤ **64 MiB** (`IMAGES_BODY_MAX_BYTES`) — guards the WASM
  engine's memory, since a decoded 8-bit RGBA frame is ~4× the pixel
  count.
- Source image ≤ **70 MP** (`IMAGES_MAX_PIXELS`), matching
  Cloudflare's documented source bound — enforced in `info()` and the
  transform gate.
- `output_quality` is validated in `1–100`; out of range →
  `err=9423`.

## Known differences from Cloudflare

- **Community engine**: jSquash is a community WASM build, not
  Cloudflare's proprietary pipeline. Encoding quality, EXIF/color-
  profile handling and resize kernel tuning will differ from
  production Cloudflare output byte-for-byte.
- **Decode coverage**: PNG, JPEG, WebP (lossy + lossless) and AVIF
  decode. GIF/BMP decode is not vendored → `err=9401`; animated
  sources decode as the first frame only, `anim` requests →
  `err=9401`.
- **Encode coverage**: `image/jpeg`, `image/png`, `image/webp`, plus
  the wire `rgb`/`rgba` raw frames. `image/avif` encoding is not
  vendored (the AVIF encoder is ~3× the size of everything else
  combined) → `err=9401`.
- **Draw coverage**: `draw_image` overlays (alpha compositing,
  `repeat` tiling, `top`/`left`/`bottom`/`right`/`width`/`height`/
  `opacity`) work; `draw_text`/`text_input` rasterization is not
  implemented → `err=9401`.
- **Gravity**: named gravity (`top`/`left`/`center`/…/`auto`) and
  `{x,y}` coordinate gravity work; face/entropy segment gravity
  (`gravity: "auto"` uses entropy-centre, not CF's ML saliency) is an
  approximation → may differ from production output.
- **Error codes**: errors ride the upstream
  `cf-images-binding: err=<code>` header. Codes reuse Cloudflare's
  numbering where a documented equivalent exists (9401 unsupported
  content/format, 9420 corrupt/undecodable, 9423 malformed request,
  9424 output too large) and reserved `94xx` values where it does not
  (9490 engine-internal).
- **`cf.image` fetch options and `/cdn-cgi/image/` URL resizing** are
  separate surfaces and remain unimplemented — this binding only
  covers the in-Worker `env.IMAGES` API.

## Examples and tests

- `vrt-local/src/project_cfg.rs` — `[images]` normalization
  (accept `binding`/`remote`, reject everything else).
- `vrt-config/tests/render.rs` — `images_binding_renders_wrapped_images_api`
  (wrapped module + inner fetcher service name).
- `facade/test/images.test.mjs` — `/info` shape cases plus end-to-end
  `/transform` coverage: real PNG→WebP resize round-trip,
  crop-gravity+rotate→JPEG, `draw_image` alpha compositing, and the
  fail-closed edges (unknown op key, GIF input, text ops, AVIF/GIF
  output).
- `shared/vrt-project-config-vectors.json` — six golden vectors:
  valid `[images]`, `remote = true` accepted-and-ignored, unknown
  sub-key, missing `binding`, non-boolean `remote`, non-table value.
