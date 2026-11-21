# Wrangler configuration support

How `vrt` reads a Cloudflare wrangler file and what it does with every
key. The parser is `normalize_wrangler_config` in
[`vrt-local/src/project_cfg.rs`](../vrt-local/src/project_cfg.rs); every
rejection is a typed `ProjectConfigError` — `VRT_CONFIG_UNSUPPORTED_FIELD`
for a key the runtime does not implement, `VRT_CONFIG_INVALID` for a
supported key with a bad value. Secret-looking values are redacted from
error messages.

## File selection

`vrt deploy` / `vrt serve` look for, in order (`WRANGLER_CANDIDATES`,
`project_cfg.rs:28`):

1. `wrangler.json`
2. `wrangler.jsonc`
3. `wrangler.toml`

A `vrt.toml` in the project root takes precedence — it is treated as
native standalone configuration and the wrangler file is not read.
`cloudflare.config.ts` and `wrangler.config.ts` are never executed and
fail closed. The file is capped at 256 KiB (`MAX_CONFIG_BYTES`).

`env.<name>` blocks are resolved before normalization:
`NON_INHERITABLE_ENV_KEYS` (`project_cfg.rs:59`) replace the top-level
value wholesale; `name`, `keep_vars` and `send_metrics` may only appear
at top level (`TOP_LEVEL_ONLY_ENV_KEYS`, `project_cfg.rs:82`).

Select a named environment with `--env` when deploying or running locally:

```sh
vrt deploy --root .vrt --env staging ./worker
vrt dev --env staging ./worker
vrt doctor --env staging
```

The same option applies to remote `vrt deploy --url` and `vrt deploy --dry-run`.
If named environments exist and `--env` is omitted, the command fails with
the available environment names instead of using the base config. `--env`
with a higher-precedence `vrt.toml` is rejected because that format has no
environment blocks.

## Behaviors

- **supported** — parsed, validated, carried to the signed snapshot.
- **accepted and ignored** — parsed (type-checked where noted) then
  dropped; mirrors wrangler's own `IGNORED_TOP_LEVEL`
  (`project_cfg.rs:31`).
- **rejected** — `VRT_CONFIG_UNSUPPORTED_FIELD` (or
  `VRT_CONFIG_INVALID` where marked); the deploy fails.

## Top-level keys

| Key | Behavior | Code location |
| --- | --- | --- |
| `name` | supported — worker name, required for standalone naming | `project_cfg.rs:540` |
| `main` | supported — entrypoint module path | `project_cfg.rs:543` |
| `compatibility_date` | supported — passed through to workerd | `project_cfg.rs:546` |
| `compatibility_flags` | supported — array of strings | `project_cfg.rs:552` |
| `vars` | supported — string table; non-strings JSON-encoded as text bindings | `project_cfg.rs:565` |
| `kv_namespaces` | supported — see [KV](bindings/kv.md); per-entry keys `binding`, `id`, `preview_id`, `remote` | `project_cfg.rs:578` |
| `d1_databases` | supported — see [D1](bindings/d1.md); per-entry keys `binding`, `database_name`, `database_id`, `preview_database_id`, `remote` | `project_cfg.rs:601` |
| `hyperdrive` | supported — see [Hyperdrive](bindings/hyperdrive.md); per-entry keys `binding`, `id`, `database_tenant`, `localConnectionString`, `remote` | `project_cfg.rs:639` |
| `ratelimits` | supported — see [Rate limiting](bindings/rate-limiting.md); `name` + `namespace_id` + `simple.{limit,period}` | `project_cfg.rs:693` |
| `r2_buckets` | supported — see [R2](bindings/r2.md); per-entry keys `binding`, `bucket_name` | `project_cfg.rs:761` |
| `services` | supported — see [Service bindings](bindings/service-bindings.md); `binding`, `service`, `entrypoint` (named entrypoint, validated at pack) | `project_cfg.rs:788` |
| `worker_loaders` | supported — see [Worker loaders](bindings/worker-loaders.md); per-entry key `binding` only | `project_cfg.rs:820` |
| `pg` | supported — vrt-native Postgres binding (`binding`, `id`, `endpoint`, `tenant_slug`, `read_only`); credentials never in toml | `project_cfg.rs:846` |
| `queues` | supported — see [Queues](bindings/queues.md); `producers` (`binding`, `queue`, `delivery_delay`) and `consumers` (`queue`, `max_batch_size`, `max_batch_timeout`, `max_retries`, `dead_letter_queue`, `retry_delay`, `max_concurrency`) | `project_cfg.rs:875` |
| `durable_objects` | supported — see [Durable Objects](bindings/durable-objects.md); `bindings[]` with `name`, `class_name`, `script_name` | `project_cfg.rs:1003` |
| `migrations` | supported — `tag`, `new_classes`, `new_sqlite_classes`, `renamed_classes` (`{from,to}`), `deleted_classes`; **`transferred_classes` rejected** | `project_cfg.rs:1062` |
| `triggers` | supported — `crons` only; see [Cron Triggers](bindings/cron-triggers.md) | `project_cfg.rs:1179` |
| `route` / `routes` | supported — pattern strings or `{ pattern }`; only plain hostnames or `<fqdn>/*`; `route` and `routes` are mutually exclusive (`VRT_CONFIG_INVALID`) | `project_cfg.rs:1202`, `route_to_hostname` `:510` |
| `assets` | supported — see [Static assets](bindings/static-assets.md); `directory`, `binding`, `html_handling`, `not_found_handling`, `run_worker_first` | `project_cfg.rs:1235` |
| `workflows` | supported — see [Workflows](bindings/workflows.md); `name`, `binding`, `class_name`, `script_name`, `limits.steps` | `project_cfg.rs:1269` |
| `tail_consumers` | supported — see [Tail Workers](bindings/tail-workers.md); `service` table or legacy string entries; distinct same-tenant already-deployed consumers, deduped, max 16; empty list valid | `project_cfg.rs:1352` |
| `limits` | supported — see [Per-worker quotas](quotas.md); `cpu_ms` (1..300000) and `subrequests` (0..10000); other keys rejected | `project_cfg.rs:1393` |
| `env` | supported — environment blocks resolved before normalization | `project_cfg.rs:1349`, env resolution earlier in the file |

## Accepted and ignored

Mirrors wrangler's own ignore list — present keys do not fail the deploy
but change nothing (`IGNORED_TOP_LEVEL`, `project_cfg.rs:31–57`):

`account_id`, `workers_dev`, `send_metrics`, `keep_vars`, `build`,
`rules`, `alias`, `tsconfig`, `jsx_factory`, `jsx_fragment`,
`no_bundle`, `minify`, `keep_names`, `find_additional_modules`,
`preserve_file_names`, `base_dir`, `define`, `upload_source_maps`,
`dev`, `cwd`, `config`, `placement`.

Also type-checked then dropped inside supported blocks:

- `hyperdrive[].id` — accepted, never used as an identity
  (`project_cfg.rs:641`).
- `hyperdrive[].localConnectionString`, `hyperdrive[].remote` —
  dev-only keys, dropped by normalization.
- `kv_namespaces[].id`, `.preview_id`, `.remote` — `id` becomes the
  local namespace identifier; `preview_id`/`remote` are dropped.
- `d1_databases[].preview_database_id`, `.remote` — dropped.
- `services[].entrypoint` — becomes the `{ service, entrypoint }` table
  form of `[bindings.service]`; the name is validated at pack
  (`worker_pkg.rs`; see [Service bindings](bindings/service-bindings.md)).

## Rejected

Any other top-level key → `VRT_CONFIG_UNSUPPORTED_FIELD`
(`project_cfg.rs:1355`):

```
[VRT_CONFIG_UNSUPPORTED_FIELD] <key> is not supported by the VRT runtime
```

That includes, non-exhaustively: `ai`, `vectorize`, `ai_search`,
`ai_search_namespaces`, `analytics_engine_datasets`, `send_email`,
`mtls_certificates`, `pipelines`, `dispatch_namespaces`, `browser`,
`containers`, `wasm_modules`, `text_blobs`, `data_blobs`,
`version_metadata`, `secrets_store_secrets`,
`unsafe`, `migrations_dir`, `observability`,
`logpush`, `site`, `preview_id` (top level),
`node_compat`, `route`-shaped `custom_domain` fields inside routes
objects (`zone_name`, `zone_id`, `custom_domain` on `routes[]` entries
are not carried — only `pattern` is read).

Inside supported blocks the rule is the same — an unrecognized nested
key names the path, e.g. `queues.consumers[0].max_ack`,
`ratelimits[0].simple.burst`, `migrations[0].transferred_classes`.

Normalized keys with no projection into the `vrt.toml` wire document
are also rejected rather than silently dropped — the projection
whitelist is `CARRIED` in `wrangler_config_to_worker_toml`
(`project_cfg.rs:1585`), the fail-closed check that produced the
"update wrangler_config_to_worker_toml" error shape.

## Changelog

- 2026-11-21: `tail_consumers` is accepted (table or legacy string
  entries, deduped, max 16) and carried to the signed snapshot as
  resolved same-tenant worker ids; empty lists are ignored.
- 2026-11-21: `queues.producers[].delivery_delay` is accepted
  (0–86,400 seconds) as a queue-level default send delay, carried into
  the signed queue binding and applied by `facade-queue` when a send
  omits `delaySeconds`.
- 2026-11-20: `vrt deploy`, `vrt dev`, and `vrt doctor` accept `--env` to select Wrangler named environments.
