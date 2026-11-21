# Images binding

`env.IMAGES` is the Workers Images binding: `info(stream)` reports
container metadata and `input(stream).transform(opts).output(opts)`
runs an image-transformation pipeline inside the platform.

**Status: Partial** — `info()` is implemented; `transform()` is wired
end to end but the pixel engine is not vendored yet, so transform
calls fail closed with a typed error instead of silently passing the
source bytes through.

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

    // Metadata without a decode — works today.
    const info = await env.IMAGES.info(body);
    // → { fileSize, format: "image/png", width, height }

    // Transform pipeline — fails closed until the WASM engine lands.
    const out = await env.IMAGES
      .input(body)
      .transform({ width: 200 })
      .output({ format: "image/webp" });
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
file field carries the bytes, `transforms` a JSON op array), and
errors ride the `cf-images-binding: err=<code>` response header that
the shim translates into a thrown `ImagesError`. No shim fork, no
parallel protocol.

## How the ops map

| op | implementation |
| --- | --- |
| `info(stream)` | Pure-TS container sniffer (`facade/images-wire.ts`) — reads PNG IHDR, JPEG SOFn, WebP VP8/VP8L/VP8X, GIF and BMP headers; SVG markup returns the dim-less `{format: "image/svg+xml"}` shape. Returns the wire `{file_size, format, width, height}` the shim maps to `fileSize`/`width`/`height`/`format`; the pixel engine never runs. |
| `input().transform()/.draw().output()` | The upstream shim serializes the op chain into the `transforms` multipart field (plus `output_format`/`output_quality`/`background`/`anim`/`text_input`/`draw_*`). The facade validates the field then returns 501 `err=9490` until the jSquash WASM engine is vendored. `input()`'s `encoding: "base64"` decode and `text()` sources are handled inside the shim already. |
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

## Known differences from Cloudflare

- **`transform()` not yet functional**: the jSquash WASM engine
  (`@jsquash/resize` + jpeg/png/webp codecs) is the planned local
  engine — a community build, not Cloudflare's proprietary pipeline.
  Encoding quality, AVIF output, animated/multi-frame images and some
  transform options will differ from production Cloudflare even after
  vendoring; this section will carry the op-coverage matrix then.
- **`info()` format coverage**: PNG, JPEG, WebP, GIF, BMP and SVG are
  recognized (SVG returns the dim-less shape like CF); AVIF and other
  containers return `err=9420` unsupported.
- **Error codes**: errors ride the upstream
  `cf-images-binding: err=<code>` header. Codes reuse Cloudflare's
  numbering where a documented equivalent exists (9401 oversized
  source, 9420 unsupported/corrupt, 9423 malformed request) and
  reserved `94xx` values where it does not (9490 engine-not-vendored).
- **`cf.image` fetch options and `/cdn-cgi/image/` URL resizing** are
  separate surfaces and remain unimplemented — this binding only
  covers the in-Worker `env.IMAGES` API.

## Examples and tests

- `vrt-local/src/project_cfg.rs` — `[images]` normalization
  (accept `binding`/`remote`, reject everything else).
- `vrt-config/tests/render.rs` — `images_binding_renders_wrapped_images_api`
  (wrapped module + inner fetcher service name).
- `shared/vrt-project-config-vectors.json` — six golden vectors:
  valid `[images]`, `remote = true` accepted-and-ignored, unknown
  sub-key, missing `binding`, non-boolean `remote`, non-table value.
