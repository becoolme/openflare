# kitesurf vendored subset — provenance

- Upstream: <https://github.com/cloudflare/kitesurf>
- Pinned commit: `0affe70c9202b80396e3c5bf147018cb760af633` (see `VERSION`)
- License: Apache-2.0 (`LICENSE` and `NOTICE` copied verbatim)
- Scope: the **built** engine worker payload (`packages/engine/dist`),
  the Docker front worker (`docker/front.js`) and the static `public/`
  tree. The TypeScript sources, Bazel build graph and test corpus are
  NOT vendored — the payload is a build artifact, pinned like workerd.

## Where the payload lives

- `facade/dist/kitesurf/engine/` — the 9-file engine `:dist` (index.js +
  hashed siblings: wasm, `.glue.js`/`.css`/`.dat` text modules). These
  are vendored INPUTS that happen to live under `dist/` so the host's
  `<facade dir>/dist/<module.name>` seeding contract covers them;
  `facade/tools/gen-manifest.mjs` never regenerates them, it only hashes
  them into `facade/manifest.json`.
- `facade/dist/kitesurf/front.js` — `docker/front.js` verbatim: routes
  requests between the engine and the `public/` disk service, and serves
  `public/` to the engine as the `FONT_ASSETS` entrypoint `Assets`.
- `facade/kitesurf-public/` — upstream `packages/engine/public/` verbatim
  (fonts, `rbi/`, `_headers`); emitted as the `facade-kitesurf-public`
  `disk` service.

## Rebuild recipe (documented, not automated)

```bash
git clone https://github.com/cloudflare/kitesurf && cd kitesurf
git checkout $(cat VERSION)
pnpm install && pnpm run build        # bazel; needs ~30 min + rustup
# payload := packages/engine/dist/*, docker/front.js, packages/engine/public/*
```

## Local deviations from the upstream Docker wiring

- `KITESURF_NATIVE_EVAL` stays empty (page eval runs on boa): openflare's
  tenant-facing workerd does not grant `experimental` per-worker.
- `subrequests` is capped at the platform maximum (10000), not upstream's
  1000000.
- The inspector frontend is not staged; `KITESURF_INSPECTOR_*` stay empty.
- `/_headers` is additionally reachable as a served file (the disk
  service has no asset-pipeline special-casing); upstream's Docker image
  has the same exposure.
