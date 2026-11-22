# Cloudflare compatibility

OpenFlare runs unmodified `workerd` and normalizes wrangler
configuration into signed deployment manifests. This page is the
platform-level summary; per-binding detail lives in
[`docs/bindings/`](bindings/README.md) and the key-by-key wrangler table
in [`docs/wrangler-config.md`](wrangler-config.md).

> Conformance comparison is **not automated yet — planned**. Every row
> below is derived by hand from the sources cited; there is no
> differential test suite against Cloudflare in this tree today.

## Summary

| Area | Status | Anchor |
| --- | --- | --- |
| Workers modules / service-worker syntax | workerd | runs the real workerd `worker` definition |
| Compatibility dates & flags | workerd | `compatibility_date`, `compatibility_flags` pass through to workerd; generated flags are checked against the pinned binary in CI |
| KV | Drop-in | [bindings/kv.md](bindings/kv.md) |
| R2 | Partial | [bindings/r2.md](bindings/r2.md) |
| D1 | Partial | [bindings/d1.md](bindings/d1.md) |
| Durable Objects | Partial | [bindings/durable-objects.md](bindings/durable-objects.md) |
| Queues | Partial | [bindings/queues.md](bindings/queues.md) |
| Cron Triggers | Drop-in | [bindings/cron-triggers.md](bindings/cron-triggers.md) |
| Workflows | Partial | [bindings/workflows.md](bindings/workflows.md) |
| Cache API | Partial | [bindings/cache-api.md](bindings/cache-api.md) |
| Service bindings | Partial | [bindings/service-bindings.md](bindings/service-bindings.md) |
| Hyperdrive | Partial | [bindings/hyperdrive.md](bindings/hyperdrive.md) |
| Rate limiting | Partial | [bindings/rate-limiting.md](bindings/rate-limiting.md) |
| Static assets | Partial | [bindings/static-assets.md](bindings/static-assets.md) |
| WebSockets | Partial | [bindings/websockets.md](bindings/websockets.md), [ingress-websocket.md](ingress-websocket.md) |
| Vars / secrets | Partial | [bindings/vars-and-secrets.md](bindings/vars-and-secrets.md) |
| Tail Workers | Partial | [bindings/tail-workers.md](bindings/tail-workers.md) |
| Flagship (`[[flagship]]` feature flags) | Partial | [wrangler-config.md](wrangler-config.md), [bindings/flagship.md](bindings/flagship.md) |
| Observability (`[observability]` Worker Logs gates + head sampling) | Partial | [wrangler-config.md](wrangler-config.md) |
| Vectorize | Partial | [bindings/vectorize.md](bindings/vectorize.md) |
| Analytics Engine | Partial | [bindings/analytics-engine.md](bindings/analytics-engine.md) |
| E-mail (`send_email`) | Partial | [bindings/send-email.md](bindings/send-email.md) |
| K2 (durable event log) | Partial | [bindings/k2.md](bindings/k2.md) |
| Workers AI, AI Search, Pipelines, mTLS, Secrets Store, Workers for Platforms/dispatch, Containers | Unsupported | [wrangler-config.md](wrangler-config.md) |

## Known platform differences

These are not tied to one binding.

- **Resource identity is server-minted.** D1 `database_id`, queue ids,
  DO `uniqueKey`, Hyperdrive binding ids, KV namespaces: the runtime
  derives them (tenant-scoped SHA-256 / HMAC) and the signed
  `compute.desired` snapshot covers the derived values — a worker bundle
  can never name an arbitrary resource id. Cloudflare ids supplied in
  wrangler (`id`, `database_id`, `bucket_name`) are treated as
  user-side display names. (`vrt-local/src/worker_pkg.rs`)
- **Scale-to-zero is structural, not behavioral.** Idle tenants are
  frozen (workerd process kept, suspended) rather than Cloudflare's
  "always warm" model — first request after an idle period pays a wake
  (`vrt-host/src/hibernate.rs`). Cron triggers and ingress wake a frozen
  tenant; DO alarms cannot, so DO/workflow tenants never freeze.
- **Tenancy.** Deployments, bindings and service targets are scoped to
  one tenant. There is no cross-account registry; `script_name` and
  `service` targets resolve inside the same tenant only.
- **No edge network.** One `vrt-host` is one ingress — no Cloudflare
  global anycast, no edge cache tier for `cacheTtl`, no Smart Placement
  (`placement` is ignored).
- **`request.cf` is a whitelist, synthesized from two sources.**
  Inbound `cf-*` headers are stripped and re-injected only when the
  request arrived through the trusted proxy → Cloudflare edge;
  verified upstream values always win. On top of that, the `[geoip]`
  module (default on) resolves the client IP the typed `clientIp`
  policy derived — or the TCP peer itself when no trusted proxy is
  configured — against a local MMDB and fills any field upstream did
  not provide. Emitted today: `country`, `colo`, `httpProtocol`,
  `asn` (JSON number), `asOrganization`, plus the visitor-location
  set `city`, `continent`, `region`, `regionCode`, `metroCode`,
  `postalCode`, `timezone`, `latitude`, `longitude`, `isEUCountry`.
  `colo` has no local source (it names the edge in front, not the
  client) and stays upstream-only; `botManagement` and the `tls*`
  fields stay absent rather than being guessed. The MMDB is the
  merged `NetworkCats/Merged-IP-Data` release — see `[geoip]` in
  `examples/*-vrt-host.toml` for mirror/disable options and the
  attribution note.

## Known limitations

- **WebSockets**: HTTP/1.1 upgrades only (no HTTP/2 extended CONNECT);
  300 s idle timeout, 4 h max session, 4096 concurrent sessions per
  ingress, 256 per tenant by default, `503`/`ws_capacity` on
  exhaustion. WebSocket hibernation is not emulated — an open socket
  keeps the tenant awake. [`docs/ingress-websocket.md`](ingress-websocket.md)
- **Durable Object `transferred_classes` rejected** at deploy
  (`VRT_CONFIG_UNSUPPORTED_FIELD`) — cross-worker storage lineage needs
  control-plane support standalone does not have. Storage is pinned to
  `(tenant, worker, class)`; rename/delete migrations are supported.
- **DO/Workflow tenants never hibernate** — alarms cannot wake a frozen
  workerd process.
- **Service binding `entrypoint`** (named entrypoints): a missing
  named export fails the publish (generation start) instead of the
  first call; `ctx.waitUntil()` inside named entrypoint classes is not
  tracked by the drain probe.
- **Cache API** is per-host, in-memory, non-persistent; `match`
  options (`ignoreMethod`, `ignoreSearch`, `ignoreVary`, `cacheName`)
  are not implemented by the pinned workerd.
- **Hyperdrive** is Postgres-only and requires the OpenFlare-only
  `database_tenant` key.
- **Rate limiting** is a per-process fixed window — approximate, not
  shared across hosts.
- **R2 `onlyIf`** depends on the backend (`s3.conditionalWrites`);
  `jurisdiction`/`storage_class` rejected. SSE-C works on the S3 backend
  only; the fs/memory backends fail closed with `R2_SSEC_INVALID`.
- **Static assets** caps: 20,000 files and 25 MiB per file (Cloudflare's free-plan limits; overridable with `VRT_ASSET_MAX_FILES` / `VRT_ASSET_MAX_FILE_BYTES`), plus a 512 MiB total guard by default (Cloudflare documents no total; override with `VRT_ASSET_MAX_TOTAL_BYTES`).
  all four `html_handling` modes, with Cloudflare's per-request-form
  candidate order and 307 canonical redirects.
- **`vars` non-string values arrive as JSON-encoded strings**, not
  typed JSON values.
- **Worker loaders** (`[[worker_loaders]]`): native `WorkerLoader`
  (`get`/`load`) on the pinned workerd fork. The loading worker
  supplies all dynamic code at runtime; `WorkerCode.limits` and
  per-entrypoint `{limits}` are enforced (lower-of-two, bounded by the
  loading worker's own limits group). Live workers per loader are
  bounded by a platform quota (32); there is no per-tenant
  `dispatch_namespaces` counterpart.
- **Tail Workers** (`tail_consumers`): rendered as native workerd
  `tails` designators alongside the platform log collector. The
  consumer must be a different, already-deployed Worker of the same
  tenant, delivery is best effort (events may be dropped and must not
  be used for billing/audit). `streaming_tail_consumers` renders
  streaming `tailStream()` consumers as workerd `streamingTails`
  designators. `TailItem.scriptName` is null
  in standalone workerd (there is no per-script name). See
  [`docs/bindings/tail-workers.md`](bindings/tail-workers.md).
- **Analytics Engine** (`[[analytics_engine_datasets]]`): native
  `writeDataPoint()` ingests into a per-tenant spool that flushes to
  canonical Parquet under `<work>/ae/out/`. Full-fidelity storage (no
  sampling/`_sample_interval`); query via DuckDB or ClickHouse
  `s3(...,'Parquet')` — see [Analytics Engine](bindings/analytics-engine.md).
- **No D1 migrations runner** — `migrations_dir` is an unsupported key.
- **Routes**: plain hostnames and `<fqdn>/*` patterns, plus CF
  `custom_domain` entries (`{ pattern = "host", custom_domain = true }` —
  the bare hostname attaches to the worker; `enabled = false` disables).
  Wildcard custom domains and zone management are not carried.

## Unsupported bindings

Any unrecognized wrangler key fails the deploy with
`VRT_CONFIG_UNSUPPORTED_FIELD` — see
[`docs/wrangler-config.md`](wrangler-config.md) for the full table.
Notable Cloudflare features with no OpenFlare counterpart: Workers AI
(`ai`), AI Search (`ai_search`,
`ai_search_namespaces`), Secrets Store
(`secrets_store_secrets`), Pipelines, dispatch namespaces
(`dispatch_namespaces`), `mtls_certificates`, `browser`,
`containers`, raw
`wasm_modules`/`text_blobs`/`data_blobs`/`version_metadata` upload keys.
`[unsafe]` is partially supported: `[[unsafe.bindings]]` entries with
`type = "unsafe_eval"` bind workerd's `unsafeEval` capability; other
`type` values and other `unsafe.*` keys fail closed.

## Error taxonomy

All deploy-time rejections are typed (`vrt-local/src/project_cfg.rs`):

- `VRT_CONFIG_INVALID` — a supported key with a malformed value.
- `VRT_CONFIG_UNSUPPORTED_FIELD` — a key OpenFlare does not implement.
- `VRT_CONFIG_IO`, `VRT_CONFIG_PARSE`, `VRT_CONFIG_ENV` — file and
  environment resolution failures.

Runtime failures visible at ingress carry platform codes documented in
[`docs/log-codes.md`](log-codes.md).

## Changelog

- 2026-11-21: Tail Workers (`tail_consumers`) are supported as native
  workerd `tails` designators; `streaming_tail_consumers` render as
  `streamingTails` designators delivering `tailStream()` events. User
  consumers coexist with the platform log collector. Delivery is best
  effort, consumers must be different same-tenant deployed Workers, and
  a worker can consume one event channel only.
- 2026-11-20: Analytics Engine (`[[analytics_engine_datasets]]` → native
  `analyticsEngine` bindings) ingests `writeDataPoint()` events to a
  per-tenant socket, spools JSONL, and flushes to canonical Parquet
  (`ae/out/<seg>/<dataset>/dt=<date>/part-*.parquet`). Query via
  DuckDB/ClickHouse; no sampling or hosted SQL API yet.

- 2026-11-20: Worker loaders (`[[worker_loaders]]` → native `WorkerLoader`
  bindings, `get`/`load`, named cache, dynamic entrypoints and DO facets)
  are now supported on the pinned forked workerd; the platform bounds live
  dynamic workers per loader.

- 2026-11-20: Re-vendored the compatibility flag schema
  (`vrt-config/compat/compatibility-date.capnp`) from the pinned workerd
  release in `workerd.lock` and added a CI check that loads every
  generated stable and experimental flag in the checksum-verified pinned
  binary, so a flag the runtime does not know fails the build instead of
  a tenant deploy.
