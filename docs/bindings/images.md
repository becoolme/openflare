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
points at `vrt-egress` (`vrt-config/src/capnp.rs`). The shim POSTs to
the synthetic host `js.images.cloudflare.com`; the egress worker
intercepts that hostname ahead of its policy evaluation and relays to
`facade-images` (`facade/egress/images-binding.ts`). For `/transform`
bodies carrying `text_input`/`draw_text` JSON specs it resolves each
spec's `font.url` through the egress policy and injects the bytes as
adjacent `text_font`/`draw_font` file fields — the facade is
deny-routed (no network egress) and cannot fetch fonts itself, and
upstream resolves font URLs service-side too. Requests without text
specs forward as raw bytes; the facade only ever receives the image
bytes the tenant worker streams in.

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
(`third_party/jsquash/`): mozjpeg, Squoosh PNG, WebP
(lossy+lossless), AVIF decode + encode, and `@jsquash/resize`
(Lanczos3/ triangle/bilinear/nearest). GIF (first frame) and BMP
(24/32bpp uncompressed) decode in pure TS (`facade/images-decode.ts`).
`.wasm` modules travel the same
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
| `input().transform()/.draw().output()` | The upstream shim serializes the op chain into the `transforms` multipart field (plus `output_format`/`output_quality`/`background`/`anim`/`text_input`/`draw_*`). The facade validates, decodes every `image`/`draw_image` file, applies the ops, flattens alpha over `background` when required, and encodes. `input()`'s `encoding: "base64"` decode is handled inside the shim already. |
| `text(content, {font:{url}, size, color})` | Shim emits a `text_input` (canvas) or `draw_text` (`.draw()` overlay) JSON spec; the egress relay fetches `font.url` through the tenant's egress policy and injects `text_font`/`draw_font` bytes; the facade rasterizes via vendored `@resvg/resvg-wasm` (fontdb + rustybuzz shaping). Spec limits: ≤1000 chars, result ≤4096×4096 px, font ≤20 MiB, `size` default 12, `color` default `#000000`. |
| `hosted.*` (`env.IMAGES.hosted.image()/.upload()/.list()/.createDirectUpload()`) | Cloudflare Images **product** RPC surface — not implemented; the facade exposes no such methods so calls reject. |

## Request-path transforms (`/cdn-cgi/image/`)

When a worker declares an `images` binding, the ingress intercepts
`/cdn-cgi/image/<options>/<src>` on its hostnames **before** the tenant
worker — the Cloudflare request-path transform surface:

```
GET /cdn-cgi/image/width=300,fit=cover,format=auto/uploads/cat.png
```

- The source is fetched **same-origin only**: `/relative/path` resolves
  through the tenant's own worker socket (worker code, assets, or the
  platform asset route). Absolute `http(s)://` URLs are accepted only
  when their host equals the request `Host` — there is no remote-fetch
  allowlist yet, so the endpoint can never proxy third-party content
  (SSRF-closed by construction). CF's allow-any-URL behavior and its
  `imageResizingAllowedHosts` zone setting are not implemented.
- The transform runs through the same `facade-images` engine: capnp
  emits a loopback `<workerId>-images.sock` service socket when the
  worker has the binding, and the ingress POSTs a multipart body
  identical to the binding wire.
- Tenants **without** an `images` binding keep `/cdn-cgi/` as an
  ordinary path (conservative gating — CF intercepts unconditionally,
  but a silent 4xx on tenants that never opted in would fork behavior).
- Options map onto the same op vocabulary. Scalar and the upstream
  structured group syntax (`param=(k=v,...)`, with `(a,b)` lists and
  percent-decoded values) are both accepted:
  `width`/`w` (px, `auto`, or `(mode=auto,mobile=,desktop=,breakpoints=(...))`
  resolved from `viewport-width`/`sec-ch-viewport-width` client hints
  or a user-agent heuristic), `height`/`h`, `dpr` (≤2 — scales
  width/height and every pixel length in trim/border/padding),
  `fit` (string mode incl. `aspect-crop`/`scale-up`, or `(mode=,upscale=)`),
  `flip` (`h`/`v`/`hv`), `gravity`/`g` (named anchors, `XxY` fractions,
  or `(x=,y=)`), `rotate`, `blur`, `sharpen` (scalar box or
  `(mode=box|gaussian,amount,sigma,threshold)`), `brightness`,
  `contrast`, `gamma`, `saturation`, `background`,
  `trim` (`color`/`border`, `T;R;B;L`/`TxRxBxL` insets, `(mode=color|area,...)`
  with px/`Np`/`0<v<1` lengths, rect form, plus the legacy dotted
  `trim.X=`/`trim.border.X=` keys), `border` (`WxCOLOR` or
  `(color=,width=|top,right,bottom,left)` with `Np` percents),
  `padding` (sides painted with `background` or transparent),
  `quality`/`q` (1–100, perceptual `low`/`medium-low`/`medium-high`/`high`,
  or `(level=,slow-connection=)` gated on `rtt`/`save-data`/`ect`/
  `downlink` client hints; `slow-connection-quality`/`scq` too),
  `compression` (`fast` → JPEG), `format`/`f` (`auto` negotiates on the
  request `Accept` header — avif > webp > jpeg), `anim`,
  `metadata` (`none`/`copyright`/`keep` ride the transform wire;
  `exif` returns the `/info` JSON), `zoom`, `segment`,
  `onerror=redirect`. Unknown or malformed keys fail closed
  `ERROR 9423`; keys the engine lacks (`zoom`, `segment`, `anim`)
  reach the facade and surface its typed `ERROR 9401`.
- Errors render in CF's own shape: `ERROR <code>` text bodies with the
  facade's status; `onerror=redirect` swaps any failure for a 302 to
  the original source path. Upstream fetch failures collapse to
  `ERROR 9411`.

## Subrequest transforms (`cf.image`)

`fetch(url, {cf: {image: {...}}})` — Cloudflare's fetch-option
transform — works end to end. workerd serializes the tenant's whole
`cf` object into the subrequest channel (`Request::serializeCfBlobJson`),
so the egress worker receives `request.cf.image` verbatim, fetches the
upstream as usual, then POSTs the response body to `facade-images` over
a `VRT_IMAGES` service binding wired unconditionally on the egress
worker (`cf.image` is a fetch option, not a declared binding — it must
transform even when the worker never declares `images`).

```js
const r = await fetch("https://cdn.example.com/cat.png", {
  cf: { image: { width: 300, fit: "cover", format: "webp", quality: 80 } },
});
// r.body is the transformed WebP, not the original PNG.
```

- Options map onto the same op vocabulary: `width`, `height`, `dpr`
  (≤2, scaling `width`/`height` and pixel lengths in `trim`/`border`/
  `padding`), `fit` (string mode incl. `aspect-crop`/`scale-up`, or
  `{mode, upscale}` — `upscale:"generate"` fails closed, no ESRGAN is
  vendored), `flip` (`"h"`/`"v"`/`"hv"`), `gravity` (named or
  `{x,y,mode}`), `rotate`, `blur`, `sharpen` (scalar box or
  `{mode, amount, sigma, threshold}` — 2026-11-19 gaussian sharpen),
  `brightness`, `contrast`, `gamma`, `saturation`, `background`,
  `trim` (`"color"`/`"border"`, `"T,R,B,L"` inset, or the structured
  `{mode:"color"|"area", ...}` forms incl. `{left:N}` inference and
  px/fraction/`"Np"` lengths), `border`/`padding` (px or per-side
  objects; `padding` paints with `background` or transparent),
  `zoom`, `segment`, `quality` (1–100 or perceptual
  `"low"`/`"medium-low"`/`"medium-high"`/`"high"`), `compression`
  (`"fast"` → JPEG, overriding `format`),
  `format` (`avif`/`webp`/`jpeg`/`baseline-jpeg`/`png`/`png-force`/
  `json`/`gif`/`rgb`/`rgba`), `anim`, `metadata`, `onerror`.
- `format: "json"` returns the **transformed** result's metadata
  (`{width, height, format, size}`), not an image body.
- Failure shape mirrors CF: transform errors return `ERROR <code>`
  text with a `cf-resized: err=<code>` header — never a silently
  untransformed image. `onerror: "redirect"` swaps any transform
  failure for a 302 to the original URL. Malformed options (unknown
  key, out-of-range value) reject `fetch()` with `TypeError`, matching
  CF's invalid-option behavior.
- `draw` accepts up to 16 entries. Image overlays (`{url,
  top/left/bottom/right, opacity, repeat, composite}` plus per-overlay
  transform options) fetch through the same egress policy as the
  source and ride `draw_image` multipart fields into the engine.
  Text overlays (`{text, font:{url}, size, color}` + the positioning/
  blending keys) fetch their font through the same policy and ride
  `draw_text` spec + `draw_font` byte pairs, sharing the
  `drawImageIndex` space with `draw_image`; per-entry transform
  options and `url` are image-entry-only → `TypeError` (upstream
  documents them on image overlays only). `image`, `origin-auth`
  and `slow-connection-quality` are recognized but unsupported →
  `TypeError` (fail closed).
- The upstream fetch still runs through the egress policy, so
  `cf.image` can only transform origins the tenant was already allowed
  to fetch — it grants no new network reach.

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
- **Decode coverage**: PNG, JPEG, WebP (lossy + lossless), AVIF, GIF
  (first frame only — pure-TS LZW decoder), and BMP (24/32bpp
  uncompressed). SVG input matches upstream split by entry point:
  the `transform()` binding rejects it (`err=9401` — upstream does
  not support SVG through the binding at all), while `/cdn-cgi/image/`
  and `cf.image` sanitize the markup with svg-hush and pass it through
  as `image/svg+xml` with every transform option ignored (malformed
  SVG → `err=9420`). An `anim` request on an animated (GIF) source →
  `err=9401` since multi-frame output is impossible.
- **Encode coverage**: `image/jpeg`, `image/png`, `image/webp`,
  `image/avif` (single-threaded encoder, speed 6), plus the wire
  `rgb`/`rgba` raw frames. `image/gif` output has no vendored encoder
  → `err=9401`.
- **Draw coverage**: `draw_image` overlays work on all three entry
  points — the binding shim's `draw()`, `cf.image.draw[]` URL
  entries, and `/cdn-cgi/` `draw=((k=v,...))` groups — with alpha
  compositing (Porter–Duff `over`/`in`/`atop`/`out`/`xor`/`lighter`),
  `repeat` tiling, `top`/`left`/`bottom`/`right`/`width`/`height`/
  `opacity`, centered placement when no edge is anchored, and
  per-overlay transform ops (fit/gravity/blur/rotate/…) including
  `"Np"` percent-of-base dimensions. `/cdn-cgi/` draw URLs stay
  same-origin like the source; `cf.image` draw URLs go through the
  egress policy. `draw_text`/`text_input` rasterize through the
  vendored resvg-wasm stack (`facade/text-rasterize.ts` — fontdb on
  wire-carried font bytes, rustybuzz shaping, tiny-skia, premultiply
  → straight-alpha conversion): `cf.image` text entries and
  `/cdn-cgi/` `draw=((text=...,font=(url=...)))` work end to end —
  the font fetches like an overlay (egress policy / same-origin),
  `size` defaults 12, `color` `#000000`, text ≤ 1000 chars, raster
  ≤ 4096². The binding's `.text()` still needs the workerd-fork
  `images-api` shim patched to fetch `font.url` into a `text_font`
  field (upstream fetches shim-side too); until then a binding text
  source fails `err=9423` for its missing font field.
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
- **`cf.image` / `/cdn-cgi/` nuances**: `cf.image` transforms any
  policy-allowed upstream (the fetch already reached it), while
  `/cdn-cgi/` stays same-origin only. `metadata` is wired on both
  paths (`none`/`copyright`/`keep`, upstream default `copyright`) —
  see `facade/images-metadata.ts` for the container surgery; AVIF
  writes EXIF only, GIF output / HEIC-TIFF sources have no
  encoder/decoder. `image`/`origin-auth` reject with `TypeError`;
  `cf.image.slow-connection-quality` is unsupported (upstream gates
  it to the URL interface anyway) and URL `format=json` is not
  implemented.
- **Perceptual quality tiers** (`low`/`medium-low`/`medium-high`/
  `high`) map to 40/60/80/90 — upstream publishes no integer mapping;
  the anchors match other CF-compat implementations.
- **`upscale: "generate"`** in the structured `fit` form needs the
  ESRGAN model — not vendored → `err=9401` (fail closed).

## Examples and tests

- `vrt-local/src/project_cfg.rs` — `[images]` normalization
  (accept `binding`/`remote`, reject everything else).
- `vrt-host/src/cdn_cgi.rs` — `/cdn-cgi/image/` ingress interception:
  option→transform JSON mapping (structured groups, percent-decoding,
  client-hints quality/width, dpr scaling), same-origin source
  resolution, `draw=((k=v,...))` group parsing with same-origin
  overlay + font fetch (`text`/`font=(url=)` entries → `draw_text`/
  `draw_font`), svg-hush sanitize pass-through for SVG sources,
  facade socket fan-out, `ERROR <code>` shaping (24 unit tests).
- `facade/svg-hush.ts` — TypeScript port of upstream's svg-hush for the
  `cf.image` egress path (the `/cdn-cgi/` path uses the crate itself):
  allowlisted elements/attributes, xlink→href, same-origin URL
  relativization, `data:` limited to image/{jpeg,png,gif}, CSS `url()`
  filtering. `facade/test/svg-hush.test.mjs` replays upstream's own
  unit-test vectors (urlfunc/css/url_filter/data:/VULN-144180) plus
  byte-exact filter outputs diffed against the crate's CLI.
- `vrt-config/tests/render.rs` — `images_binding_renders_wrapped_images_api`
  (wrapped module + inner fetcher service name).
- `facade/test/images.test.mjs` — `/info` shape cases plus end-to-end
  `/transform` coverage: real PNG→WebP resize round-trip,
  crop-gravity+rotate→JPEG, `draw_image` alpha compositing,
  `text_input`/`draw_text` rasterization and field-pairing rules,
  GIF/BMP input decode, AVIF output, and the fail-closed edges
  (unknown op key, corrupt input, `anim`, GIF output).
- `facade/test/text-rasterize.test.mjs` — `text-spec` validation
  vectors (spec JSON, CSS color shapes, font magics) and resvg-wasm
  rasterization on an OFL-licensed test font
  (`facade/test/fixtures/test-sans.ttf`).
- `facade/test/images-structured.test.mjs` — the structured-parameter
  surface on real pixels: box vs gaussian sharpen (incl. `threshold`
  gating), `trim` color/area/rect/percent/fraction forms,
  `aspect-crop`/`scale-up` geometry, `flip`, percent borders.
- `facade/test/images-metadata.test.mjs` — `metadata` keep/copyright/
  none across JPEG/PNG/WebP/AVIF containers incl. IPTC→XMP mapping.
- `vrt-host/tests/images_workerd.rs` — real-workerd smoke (`VRT_WORKERD_BIN`
  gated): the rendered `wasm = embed` modules instantiate in the
  pinned binary and `env.IMAGES` info/transform round-trip through the
  actual shim + fetcher wiring.
- `shared/vrt-project-config-vectors.json` — six golden vectors:
  valid `[images]`, `remote = true` accepted-and-ignored, unknown
  sub-key, missing `binding`, non-boolean `remote`, non-table value.
