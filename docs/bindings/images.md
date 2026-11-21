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
`cloudflare-internal:images-api` — the same internal module name
upstream Miniflare uses — whose inner `fetcher` service binding points
at the `facade-images` system worker (`vrt-config/src/capnp.rs`,
`facade/facade-images.ts`). The facade is deny-routed (no network
egress): it only receives the image bytes the tenant worker streams
in.

## How the ops map

| op | implementation |
| --- | --- |
| `info(stream)` | Pure-TS container sniffer (`facade/images-wire.ts`) — reads PNG IHDR, JPEG SOFn, WebP VP8/VP8L/VP8X, GIF and BMP headers. Returns `{fileSize, format, width, height}`; the pixel engine never runs. |
| `input().transform().output()` | Routes `POST /transform` to the facade with params on `x-vrt-images-params`. Returns 501 `9490` ("transform engine not vendored in this build") until the jSquash WASM engine is vendored. |

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
- At runtime the facade answers the Images error envelope
  `{error: {code, message}}` with an HTTP status; the
  `cloudflare-internal:images-api` shim turns it into a thrown
  `Error` carrying `.code`.

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
- **`info()` format coverage**: PNG, JPEG, WebP, GIF and BMP are
  recognized; `avif`/`svg` names exist in the type but no sniffer yet
  (returns 9420 unsupported).
- **Error codes**: the facade returns the Images error envelope
  `{error: {code, message}}`; codes reuse Cloudflare's numbering where
  a documented equivalent exists (9401 oversized source, 9420
  unsupported/corrupt, 9423 malformed params) and reserved `94xx`
  values where it does not (9490 engine-not-vendored).
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
