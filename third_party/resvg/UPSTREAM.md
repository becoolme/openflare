# resvg-wasm vendored text rasterizer — provenance

- Upstream: <https://github.com/yisibl/resvg-js> (published to npm as
  `@resvg/resvg-wasm`)
- Pinned npm tarball: `@resvg/resvg-wasm` 2.6.2
  (`index.js` + `index_bg.wasm`, ~2.5 MB)
- License: MPL-2.0 (`LICENSE` copied verbatim from the resvg-js repo).
- Scope: the `.js` glue was converted from CJS to ESM (the `__export`
  boilerplate and `module.exports` tail removed; `initWasm` and `Resvg`
  are exported directly). The `.wasm` is untouched.
- Composition: fontdb parses ttf/otf/woff/woff2 from caller-supplied
  bytes (`fontBuffers`, `loadSystemFonts: false`), rustybuzz shapes,
  tiny-skia rasterizes — no system fonts, no bundled font assets.

## Why

`text_input` / `draw_text` overlays rasterize text to an RGBA frame
inside `facade-images` (see `facade/text-rasterize.ts`). Fonts arrive
over the wire like overlay images (`text_font` / `draw_font` fields),
matching upstream's `font: {url}` fetch model, so no font assets ship
with the platform. resvg's public API gives us the tight bounding box
(`innerBBox` + `cropByBBox`) the transparent text canvas needs.

## Layout

```
index.js       wasm-bindgen glue (ESM-converted)
index_bg.wasm  the resvg pipeline binary
```
