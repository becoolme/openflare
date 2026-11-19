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
at top level (`TOP_LEVEL_ONLY_ENV_KEYS`, `project_cfg.rs:81`).

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
| `name` | supported — worker name, required for standalone naming | `project_cfg.rs:524` |
| `main` | supported — entrypoint module path | `project_cfg.rs:527` |
| `compatibility_date` | supported — passed through to workerd | `project_cfg.rs:530` |
| `compatibility_flags` | supported — array of strings | `project_cfg.rs:536` |
| `vars` | supported — string table; non-strings JSON-encoded as text bindings | `project_cfg.rs:549` |
| `kv_namespaces` | supported — see [KV](bindings/kv.md); per-entry keys `binding`, `id`, `preview_id`, `remote` | `project_cfg.rs:562` |
| `d1_databases` | supported — see [D1](bindings/d1.md); per-entry keys `binding`, `database_name`, `database_id`, `preview_database_id`, `remote` | `project_cfg.rs:585` |
| `hyperdrive` | supported — see [Hyperdrive](bindings/hyperdrive.md); per-entry keys `binding`, `id`, `database_tenant`, `localConnectionString`, `remote` | `project_cfg.rs:623` |
| `ratelimits` | supported — see [Rate limiting](bindings/rate-limiting.md); `name` + `namespace_id` + `simple.{limit,period}` | `project_cfg.rs:677` |
| `r2_buckets` | supported — see [R2](bindings/r2.md); per-entry keys `binding`, `bucket_name` | `project_cfg.rs:745` |
| `services` | supported — see [Service bindings](bindings/service-bindings.md); `binding`, `service`, `entrypoint` (entrypoint rejected later at pack) | `project_cfg.rs:772` |
| `pg` | supported — vrt-native Postgres binding (`binding`, `id`, `endpoint`, `tenant_slug`, `read_only`); credentials never in toml | `project_cfg.rs:804` |
| `queues` | supported — see [Queues](bindings/queues.md); `producers` (`binding`, `queue`) and `consumers` (`queue`, `max_batch_size`, `max_batch_timeout`, `max_retries`, `dead_letter_queue`, `retry_delay`, `max_concurrency`) | `project_cfg.rs:833` |
| `durable_objects` | supported — see [Durable Objects](bindings/durable-objects.md); `bindings[]` with `name`, `class_name`, `script_name` | `project_cfg.rs:961` |
| `migrations` | supported — `tag`, `new_classes`, `new_sqlite_classes`, `renamed_classes` (`{from,to}`), `deleted_classes`; **`transferred_classes` rejected** | `project_cfg.rs:1020` |
| `triggers` | supported — `crons` only; see [Cron Triggers](bindings/cron-triggers.md) | `project_cfg.rs:1137` |
| `route` / `routes` | supported — pattern strings or `{ pattern }`; only plain hostnames or `<fqdn>/*`; `route` and `routes` are mutually exclusive (`VRT_CONFIG_INVALID`) | `project_cfg.rs:1160`, `route_to_hostname` `:494` |
| `assets` | supported — see [Static assets](bindings/static-assets.md); `directory`, `binding`, `html_handling`, `not_found_handling`, `run_worker_first` | `project_cfg.rs:1193` |
| `workflows` | supported — see [Workflows](bindings/workflows.md); `name`, `binding`, `class_name`, `script_name`, `limits.steps` | `project_cfg.rs:1227` |
| `env` | supported — environment blocks resolved before normalization | `project_cfg.rs:1307`, env resolution earlier in the file |

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
  (`project_cfg.rs:625`).
- `hyperdrive[].localConnectionString`, `hyperdrive[].remote` —
  dev-only keys, dropped by normalization.
- `kv_namespaces[].id`, `.preview_id`, `.remote` — `id` becomes the
  local namespace identifier; `preview_id`/`remote` are dropped.
- `d1_databases[].preview_database_id`, `.remote` — dropped.
- `services[].entrypoint` — parsed into the normalized document but
  rejected at pack time because the standalone wire shape has no
  entrypoint field (`worker_pkg.rs`; see
  [Service bindings](bindings/service-bindings.md)).

## Rejected

Any other top-level key → `VRT_CONFIG_UNSUPPORTED_FIELD`
(`project_cfg.rs:1313`):

```
[VRT_CONFIG_UNSUPPORTED_FIELD] <key> is not supported by the VRT runtime
```

That includes, non-exhaustively: `ai`, `vectorize`, `ai_search`,
`ai_search_namespaces`, `analytics_engine_datasets`, `send_email`,
`mtls_certificates`, `pipelines`, `dispatch_namespaces`, `browser`,
`containers`, `wasm_modules`, `text_blobs`, `data_blobs`,
`version_metadata`, `tail_consumers`, `secrets_store_secrets`,
`worker_loaders`, `unsafe`, `migrations_dir`, `observability`,
`limits` (top level), `logpush`, `site`, `preview_id` (top level),
`node_compat`, `route`-shaped `custom_domain` fields inside routes
objects (`zone_name`, `zone_id`, `custom_domain` on `routes[]` entries
are not carried — only `pattern` is read).

Inside supported blocks the rule is the same — an unrecognized nested
key names the path, e.g. `queues.producers[0].delivery_delay`,
`ratelimits[0].simple.burst`, `migrations[0].transferred_classes`.

Normalized keys with no projection into the `vrt.toml` wire document
are also rejected rather than silently dropped — the projection
whitelist is `CARRIED` in `wrangler_config_to_worker_toml`
(`project_cfg.rs:1543`), the fail-closed check that produced the
"update wrangler_config_to_worker_toml" error shape.
