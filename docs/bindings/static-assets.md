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

Any of Cloudflare's four `html_handling` modes can be added to that
block (the example keeps the default `auto-trailing-slash`):

```jsonc
{
  "assets": {
    "directory": "public",
    "html_handling": "drop-trailing-slash" // auto-trailing-slash (default) | force-trailing-slash | drop-trailing-slash | none
  }
}
```

Native `vrt.toml` equivalent (`examples/standalone-kv-assets/vrt.toml`):

```toml
[assets]
dir = "public"
binding = "ASSETS"                    # optional — defaults to ASSETS
notFoundHandling = "single-page-application"
htmlHandling = "auto-trailing-slash"  # optional — same four modes
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

- `html_handling` accepts all four Cloudflare modes
  (`auto-trailing-slash`, `force-trailing-slash`, `drop-trailing-slash`,
  `none`) and follows Cloudflare's resolver
  (`asset-worker/src/handler.ts`) row for row: the request *form*
  (`.html`, `/index`, trailing slash, bare path) picks the candidate
  order, overlapping `/foo.html` + `/foo/index.html` keep their own
  URLs, and a canonical rewrite is only emitted when its destination
  resolves back to the same asset (`safeRedirect`) — otherwise the
  exact URL is served. Non-canonical forms return **307** with a
  `Location` header built from the percent-encoded path plus the
  original query string; dotted stems (`/release-1.2` →
  `/release-1.2.html`) and exact extensionless files (`/api/index`) are
  served normally. Non-HTML assets (`/app.js`) are served verbatim and
  never rewritten. Redirect targets are always origin-relative:
  repeated slashes are collapsed like Cloudflare's `decodePath`, so
  `/%2Fevil.com` redirects to `/evil.com` and never to a
  protocol-relative `//evil.com`.
- `not_found_handling` accepts `none`, `404-page`,
  `single-page-application`.
- `_redirects` is parsed and applied before asset lookup (Cloudflare
  order): static rules first, then dynamic ones; 301/302/303/307/308,
  one trailing `*` (`:splat`), segment placeholders (`:name`), and
  `200` proxying to a relative path. Query-parameter/country/cookie
  conditions, domain-level rules and rewrites other than 200 are not
  supported. `_headers` is supported: rules apply in file order to
  asset responses (not Worker responses), multiple matches accumulate,
  `! Name` detaches earlier values, `:splat`/`:name` substitute in
  values (in one pass, `:name` may be combined with a trailing `*`),
  and duplicates are comma-joined. `_headers`/`_redirects` are
  configuration and are never served as assets — neither directly nor
  through a `200` rewrite destination. The assets facade only forwards
  the names the provider marks as `_headers`-touched, so
  backend-internal headers can never leak.
- `_headers`/`_redirects` are validated at pack time
  (`vrt-local/src/worker_pkg.rs`, both the directory scan and the
  upload path): ≤ 512 KiB, valid UTF-8, and — for `_headers` — at most
  256 distinct / 4 KiB of forwarded header names. The provider reads
  these files with those smaller caps, so shipping an unusable one
  would otherwise fail closed on every asset request (or silently drop
  the whole forwarding list) after a successful deploy.
- `_redirects` matching runs on the raw (still percent-encoded) request
  path, like Cloudflare: capture values are substituted into `Location`
  verbatim, so `/en/%2Fevil.com` rewrites to `/%2Fevil.com` instead of a
  protocol-relative `//evil.com`, and `%3F`/`%23` inside a filename keep
  their meaning. A capture that starts a new segment is never allowed to
  turn a relative destination into a protocol-relative URL
  (`/en//evil.com` + `/:splat` → `/evil.com`); an explicitly authored
  `//host/...` destination is preserved. Placeholders are
  substituted in one pass (longest-match, inserted values never
  rescanned). `facade-router` matches the same raw pathname so routing
  and serving agree.
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
