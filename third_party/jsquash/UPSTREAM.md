# jSquash vendored codecs — provenance

- Upstream: <https://github.com/jamsinclair/jSquash> (packages published to
  npm as `@jsquash/*`)
- Pinned npm tarballs:
  - `@jsquash/resize` 2.1.1 (`squoosh_resize.js`, `squoosh_resize_bg.wasm`)
  - `@jsquash/jpeg` 1.6.0 (`mozjpeg_dec.js/.wasm`, `mozjpeg_enc.js/.wasm`)
  - `@jsquash/png` 3.1.1 (`squoosh_png.js`, `squoosh_png_bg.wasm`)
  - `@jsquash/webp` 1.5.0 (`webp_dec.js/.wasm`, `webp_enc_simd.js/.wasm`)
  - `@jsquash/avif` 2.1.1 (`avif_dec.js/.wasm` — decode only; the encoder is
    ~3.3 MB and deliberately not vendored)
- License: Apache-2.0 (`LICENSE` copied verbatim from the jSquash repo).
- Scope: exactly the `.js` glue + `.wasm` pairs the facade images engine
  (`facade/images-engine.ts`) imports. Nothing is patched; the glue is used
  through the `instantiateWasm` / `init(WebAssembly.Module)` hooks the
  packages ship for Cloudflare Workers.

## Why

`facade-images` runs `env.IMAGES.input().transform().output()` inside the
system worker — workerd has no DOM canvas, so decode/resize/encode needs a
pure-WASM engine. jSquash is the maintained community build of the Squoosh
codecs; it is not Cloudflare's proprietary pipeline (documented in
`docs/bindings/images.md` "Known differences").

## Layout

```
avif/    avif_dec.js + .wasm          (decode)
jpeg/    mozjpeg_dec/enc.js + .wasm   (decode + encode)
png/     squoosh_png.js + _bg.wasm    (decode + encode, wasm-bindgen glue)
resize/  squoosh_resize.js + _bg.wasm (lanczos3/triangle/… resize)
webp/    webp_dec + webp_enc_simd     (decode + encode)
```

The `.wasm` files are raw modules — `facade/tools/gen-manifest.mjs` emits
them byte-for-byte with manifest `"type": "wasm"`, and the capnp renderer
embeds them as `wasm = embed` system modules.
