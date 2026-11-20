# Static assets

An asset directory packed at deploy, content-addressed, and served
through `facade-assets` — optionally routed assets-first by
`facade-router`.

**Status: Partial**

## Wrangler configuration

From `examples/standalone-wrangler/wrangler.jsonc`:

```jsonc
{
  "assets": {
    "directory": "public",
    "binding": "ASSETS",
    "not_found_handling": "404-page"
  }
}
```

Native `vrt.toml` equivalent (`examples/standalone-kv-assets/vrt.toml`):

```toml
[assets]
dir = "public"
binding = "ASSETS"                    # optional — defaults to ASSETS
notFoundHandling = "single-page-application"
# assetsFirst = true                  # opt into Cloudflare assets-first routing
```

## Where it runs

- At deploy, `[assets]` is scanned into a canonical manifest plus
  per-file sha256 objects in the content-addressed store shared by
  `vrt-local` and `vrt-provider` (`vrt-local/src/worker_pkg.rs`).
- `facade-assets` (`AssetTransport`) serves `env.<binding>.fetch()` —
  GET/HEAD only — from the provider's `/internal/assets/v1/fetch`,
  `route-index` and `can-fetch` routes.
- `facade-router` (`AssetsRouter`) sits in front of the user worker when
  the deployment opts into assets-first routing: route-index hit → the
  assets binding; miss → the worker; index/RPC failures fail closed into
  typed platform codes (`route_index_*`, `can_fetch_*` in
  `docs/log-codes.md`).

## Backends

Content-addressed fs store (`[assets] dir` on both `vrt-provider` and
`vrt-host`): `<dir>/<tenant>/manifests/<digest>.json` and
`<dir>/<tenant>/objects/<sha256>`. Deploy writes are verified — a digest
that does not match the bytes is never committed.

## Known differences from Cloudflare

- `html_handling` accepts only `auto-trailing-slash` and `none`;
  Cloudflare's `force-trailing-slash` / `drop-trailing-slash` are
  rejected at pack (`vrt-local/src/worker_pkg.rs`).
- `not_found_handling` accepts `none`, `404-page`,
  `single-page-application`.
- `run_worker_first`: `true`/`false` or a list of `/`-prefixed globs
  with `!` asset-first exclusions — at most 100 rules × 100 bytes, no
  duplicates, at least one non-`!` rule (validated identically in the
  packer and the snapshot validator).
- Routing defaults differ by config source: a wrangler-family deploy
  behaves like Cloudflare (assets-first, `run_worker_first` overrides);
  native `vrt.toml` defaults to worker-first unless `assetsFirst = true`
  — preserving pre-T31c standalone behavior.
- Pack-time caps: ≤ 512 files, ≤ 1 MiB per file, ≤ 8 MiB total
  (`vrt-local/src/worker_pkg.rs`). Symlinks are rejected; dotfiles are
  skipped.
- MIME types come from a fixed extension table; unknown extensions map
  to `application/octet-stream`.

## Unsupported keys and error shape

`assets` accepts only `directory`, `binding`, `html_handling`,
`not_found_handling`, `run_worker_first` — anything else →
`VRT_CONFIG_UNSUPPORTED_FIELD` (`assets.<key>`). A missing `directory` →
`VRT_CONFIG_INVALID`; invalid `run_worker_first` rules →
`VRT_CONFIG_INVALID` (`assets.run_worker_first`).

## Examples and tests

- `examples/standalone-kv-assets` — assets dir + SPA fallback on
  `vrt.toml`.
- `examples/standalone-wrangler` — `assets` block in `wrangler.jsonc`
  with `404-page` handling.
- `facade/test/assets.test.mjs`, `facade/test/router.test.mjs`,
  `facade/test/router-contract.test.mjs` — facade asset/router contract
  tests.
- `vrt-provider/tests/assets_fetch.rs` — provider assets route tests.
