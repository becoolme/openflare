# Binding support matrix

Every binding OpenFlare declares, what serves it, and how closely it
matches Cloudflare. Each row links to a page with the wrangler
configuration shape, the component that runs it, its backends, and every
known difference.

Status tiers (same vocabulary as openorange, adapted to this codebase):

- **Drop-in** — works as on Cloudflare. The documented configuration and
  API surface behave the same; differences are limited to where the data
  lives.
- **Partial** — runs with known differences, listed on the binding's
  page.
- **Unsupported** — the configuration key fails the deploy with
  `VRT_CONFIG_UNSUPPORTED_FIELD` (see
  [`docs/wrangler-config.md`](../wrangler-config.md)).

There is no "pluggable" tier: OpenFlare ships fixed backends selected by
host/provider configuration, not per-binding provider plug-ins.

| Binding | Status | Why |
| --- | --- | --- |
| [KV](kv.md) | Drop-in | Full `KVNamespace` surface (get, getWithMetadata, bulk get, put with expiration/metadata, delete, list) on filesystem or Kvrocks. `cacheTtl` is validated but there is no edge-cache tier to honour it with. |
| [R2](r2.md) | Partial | head/get/put/delete/list plus the full multipart chain. Conditional writes (`onlyIf`) depend on the backend (`s3.conditionalWrites`); `jurisdiction`, `storage_class` and other `r2_buckets` keys are rejected. |
| [D1](d1.md) | Partial | workerd's own `d1-api` surface on top of `vrt-sql` (single-writer SQLite, fs object persistence). `database_id`/`database_name` are user-side identifiers; the real database id is server-derived. No Time Travel. |
| [Durable Objects](durable-objects.md) | Partial | SQLite storage (`new_sqlite_classes`, `new_classes` on the same store), alarms, `renamed_classes`/`deleted_classes`, same-tenant `script_name`. `transferred_classes` is rejected; a DO tenant never hibernates or scales to zero. |
| [Queues](queues.md) | Partial | Producers and consumers with retries, retry delay, concurrency caps and dead-letter queues on `vrt-sql`. Queue names are display names; ids are server-derived; delivery is at-least-once through a dispatcher. |
| [Cron Triggers](cron-triggers.md) | Drop-in | `triggers.crons` (5-field expressions) delivered to `scheduled()` at most once per `(worker, version, scheduledTime)`; cron triggers wake a hibernated tenant. |
| [Workflows](workflows.md) | Partial | Vendored Cloudflare engine (miniflare lineage) running as a Durable Object in the tenant workerd. `class_name` must export a `WorkflowEntrypoint` subclass (deploy-time gate); `limits.steps` capped at 100,000. |
| [Cache API](cache-api.md) | Partial | `caches.default` / `caches.open` on a per-tenant, in-memory service. Not persistent; requires `[cache]` in the host config. |
| [Service bindings](service-bindings.md) | Partial | Worker-to-worker bindings inside one tenant: default export or a named `WorkerEntrypoint` via `services[].entrypoint`. |
| [Hyperdrive](hyperdrive.md) | Partial | Postgres wire protocol through the egress layer to `vrt-provider`'s loopback proxy; credentials never enter the isolate. Requires the `database_tenant` extension key; Postgres only, no MySQL. |
| [Rate limiting](rate-limiting.md) | Partial | `[[ratelimits]]` with `namespace_id` + `simple.limit`/`period`. Fixed-window counter inside the per-tenant facade worker — approximate, per process, not shared across hosts. |
| [Static assets](static-assets.md) | Partial | `assets.directory`/`binding`/`not_found_handling`/`run_worker_first`, content-addressed store. `html_handling` supports only `auto-trailing-slash` and `none`; per-deploy caps of 512 files / 1 MiB each / 8 MiB total. |
| [WebSockets](websockets.md) | Partial | HTTP/1.1 upgrade passthrough to workers and Durable Objects. 300 s idle / 4 h max session, 4096 sessions per ingress, 256 per tenant. |
| [Vars and secrets](vars-and-secrets.md) | Partial | `vars` become `text` bindings, so non-string values arrive JSON-encoded as strings. Secrets are `vrt secret put` + `[secrets]` name→ref maps injected as process env vars; `secrets_store_secrets` is rejected. |
| Workers AI (`ai`) | Unsupported | Any unrecognized top-level key fails the deploy with `VRT_CONFIG_UNSUPPORTED_FIELD`. |
| Vectorize (`vectorize`) | Unsupported | Same typed deploy error. |
| `ai_search` / `ai_search_namespaces` | Unsupported | Same typed deploy error. |
| Tail Workers (`tail_consumers`) | Unsupported | Same typed deploy error. (OpenFlare uses an internal tail worker per tenant for logs; it is not user-configurable.) |
| Analytics Engine (`analytics_engine_datasets`) | Unsupported | Same typed deploy error. |
| `send_email`, `mtls_certificates`, `pipelines`, `dispatch_namespaces`, `browser`, `containers`, `wasm_modules`, `text_blobs`, `data_blobs`, `version_metadata`, `unsafe`, `placement.mode`, `secrets_store_secrets`, `worker_loaders` | Unsupported | Same typed deploy error — the key names the field, e.g. `[VRT_CONFIG_UNSUPPORTED_FIELD] ai is not supported by the VRT runtime`. |

The definitive list of accepted wrangler keys lives in
[`docs/wrangler-config.md`](../wrangler-config.md); the platform-level
differences that are not tied to one binding live in
[`docs/compatibility.md`](../compatibility.md).
