# OpenFlare website

The project landing page, built with [Astro](https://astro.build) as a fully
static site and served from Cloudflare Workers static assets.

```
npm install
npm run dev       # local dev server (astro dev)
npm run build     # static output in dist/
npm run preview   # serve dist/ through wrangler, same as production
npm run deploy    # build, then wrangler deploy
```

`wrangler.jsonc` declares an assets-only Worker: there is no `main` script, so
every request is answered from `dist/`. Unknown paths get `404.html` with a 404
status (`not_found_handling: "404-page"`).

## Layout

- `src/pages/index.astro` assembles the page from section components.
- `src/components/*.astro` hold one section each (hero, request path,
  compatibility, quick start, …). The diagrams are inline SVG.
- `src/data/content.ts` holds the copy that repeats: request steps, binding
  tiles, limits, quick-start commands and FAQ.

Copy on this page must describe what the repository actually does. When a
binding, limit or CLI command changes, update `src/data/content.ts` in the same
change.
