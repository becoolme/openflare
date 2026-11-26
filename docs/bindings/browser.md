# Browser

`env.<binding>` is the `Fetcher` `@cloudflare/puppeteer` drives —
rendered as a plain service binding to the `facade-browser` system
worker's `BrowserBinding` entrypoint, which implements Cloudflare's
Browser Rendering `/v1/*` protocol and bridges it onto the vendored
kitesurf engine (`third_party/kitesurf`).

**Status: Partial** — `browser-basic` in `conformance/` verifies
`puppeteer.launch`/`connect`, `setContent` navigation and screenshots
end to end against real workerd, including a PNG golden; the deeper
CDP surface is unverified. The engine is kitesurf (Blitz/Stylo/Boa/
Krilla compiled to wasm), not Chromium: WebGL/WebAudio/WebRTC/video/
anti-bot fingerprints, service workers, iframe JS and synchronous XHR
are out of scope upstream.

## Wrangler configuration

```toml
[browser]
binding = "BROWSER"
```

Native `vrt.toml` equivalent — a name list (no resource reference;
sessions are tenant-scoped via the binding's signed props):

```toml
[bindings]
browser = ["BROWSER"]
```

`binding` is the only accepted field. `remote` (a CF local-dev knob)
is accepted as a boolean and ignored — the engine always runs local
inside the platform. Other keys fail closed
(`VRT_CONFIG_UNSUPPORTED_FIELD browser.<key>`).

## Using it

```js
import puppeteer from "@cloudflare/puppeteer";

export default {
  async fetch(req, env) {
    const browser = await puppeteer.launch(env.BROWSER);
    const page = await browser.newPage();
    await page.goto("https://example.com");
    const png = await page.screenshot();
    await browser.close();
    return new Response(png, { headers: { "content-type": "image/png" } });
  },
};
```

`puppeteer.launch(env.BROWSER)` POSTs `/v1/devtools/browser` for a
`{sessionId}` then upgrades `/v1/devtools/browser/<id>`; CDP runs
verbatim over that socket. `puppeteer.connect(env.BROWSER, sessionId)`
re-attaches to a live session. `/v1/sessions`, `/v1/history` and
`/v1/limits` are served too (management API shape).

## Where it runs

`env.BROWSER` → `facade-browser` (`BrowserBinding` entrypoint):

- the `/v1/*` protocol layer — acquire/attach/close, `keep_alive`
  parsing and clamping, the per-tenant session registry (live +
  bounded history), and `/json/*` passthrough;
- `ENGINE` service binding → `facade-kitesurf-front` → `facade-kitesurf`,
  the vendored engine worker whose `env.LOADER` spawns a fresh
  PageScript isolate per navigation.

Sessions are per-tenant: the registry is keyed by the signed
`instanceId` props on the service binding, so tenant A can never see
or attach tenant B's `sessionId`s.

## Backends

The engine selector is `?browser=kitesurf` — the default and the only
value accepted. `?browser=chromium` (and any other engine name) fails
closed with `400` until a second engine ships; the selector exists so
the protocol surface already matches CF.

Engine provenance: `third_party/kitesurf` (Apache-2.0, pinned upstream
commit recorded in `third_party/kitesurf/UPSTREAM.md`); built modules
live in `facade/dist/kitesurf/`, font/assets payload in
`facade/kitesurf-public/`.

## Limits

Per tenant, enforced by `facade-browser` (override via the manifest's
`BROWSER_LIMITS` json binding):

- 4 concurrent sessions, 15 min max session age, 10 min max
  `keep_alive`, 60 s pending-attach TTL, 100-entry history.

Attach semantics: unknown sessionId → `404`, known-but-closed → `410`,
already-attached → `409`. A `keep_alive` session outlives its first
socket only until the keep-alive window or session cap.

## Known differences from Cloudflare

- **Engine.** CF production runs Chromium; VRT runs kitesurf —
  Cloudflare's own open-source wasm browser engine, which upstream
  itself calls a proof of concept. Rendering coverage gaps are
  upstream's (see `third_party/kitesurf/UPSTREAM.md` for the pin);
  anything requiring real Chromium behaviour (GPU raster, media,
  fingerprints) is absent.
- **Session persistence.** A kitesurf session lives on its WebSocket;
  `facade-browser` adds `keep_alive` reattach on top, but sessions do
  not survive a host restart (no durable storage — same as CF's
  semantics where sessions are best-effort).
- **`browser.close` on `connect`.** Mirrors CF: closing a `connect`ed
  client releases the attachment; the session ends only when it was
  `launch`ed or the keep-alive/age limit hits.

## Unsupported keys and error shape

Only `binding` (+ ignored `remote`) is accepted:

```
[VRT_CONFIG_UNSUPPORTED_FIELD] browser.lab is not supported by the VRT runtime
```

Unsupported engine selection is a protocol error, not a config error:
`POST /v1/devtools/browser?browser=chromium` → `400`.

## Examples and tests

- `conformance/cases/browser/browser-basic/` — end-to-end against real
  workerd: `launch`/`connect` acquire+attach, `setContent` +
  `page.screenshot` PNG golden, `/v1/{sessions,history,limits}` control
  surface, `?browser=chromium` fail-closed.
- `facade/test/browser.test.mjs` — acquire/attach/close lifecycle,
  404/410/409 attach semantics, concurrency cap + pending sweep,
  per-tenant registry isolation, `/json/*` passthrough.
- `vrt-config/tests/render.rs` `browser_binding_renders_service_to_facade_browser`
  — service-binding emission to `facade-browser`/`BrowserBinding`.
- `vrt-local` `browser_binding_into_pack_shape` — wrangler `[browser]`
  normalize + pack shape (`binding` only, `remote` ignored, unknown
  keys rejected).
