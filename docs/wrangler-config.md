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
| `services` | supported — see [Service bindings](bindings/service-bindings.md); `binding`, `service`, `entrypoint` (named entrypoint), `props` (JSON object for `ctx.props`, max 8 KiB); validated at pack | `project_cfg.rs:788` |
| `worker_loaders` | supported — see [Worker loaders](bindings/worker-loaders.md); per-entry key `binding` only | `project_cfg.rs:820` |
| `analytics_engine_datasets` | supported — see [Analytics Engine](bindings/analytics-engine.md); per-entry keys `binding`, `dataset`, `remote` | `project_cfg.rs:821` |
| `pg` | supported — vrt-native Postgres binding (`binding`, `id`, `endpoint`, `tenant_slug`, `read_only`); credentials never in toml | `project_cfg.rs:846` |
| `queues` | supported — see [Queues](bindings/queues.md); `producers` (`binding`, `queue`, `delivery_delay`) and `consumers` (`queue`, `max_batch_size`, `max_batch_timeout`, `max_retries`, `dead_letter_queue`, `retry_delay`, `max_concurrency`) | `project_cfg.rs:875` |
| `unsafe` | partial — `[[unsafe.bindings]]` with `type = "unsafe_eval"` only: `env.<name>` is workerd's `UnsafeEval` object (`.eval`, `.newFunction`, `.newAsyncFunction`, `.newWasmModule`); global `eval`/`new Function` stay blocked in request handlers. Other `type` values fail closed (every other binding kind has a first-class key or no runtime support); other `[unsafe]` keys (`capnp`, `metadata`, …) inject raw workerd/upload structures and are refused | `project_cfg.rs:972` |
| `observability` | partial — Worker Logs gates + head sampling applied by the platform `vrt-tail` collector: `enabled`/`logs.enabled` `false` renders NO collector (nothing collected); `logs.head_sampling_rate` (or the deprecated top-level `head_sampling_rate` alias; when both are present `logs.*` wins, matching CF's authoritative slot) keeps that fraction of INVOCATIONS whole, decided per invocation (a dropped invocation keeps none of its records; deterministic FNV-1a over `scriptName`+`eventTimestamp`+`durableObjectId`); the rate crosses the signed wire as integer ppm `headSamplingRatePpm` (0..1_000_000 — canonical JSON is safe-integer-only), so effective granularity is 1e-6; `logs.invocation_logs` `false` drops only the outcome record. The table inherits into `[env.*]` like upstream wrangler (it is NOT in `NON_INHERITABLE_ENV_KEYS`) and an env block deep-merges onto the inherited table, so a top-level `enabled = false` or `logs.invocation_logs = false` stays in force in every named environment unless that environment overrides the exact key. `traces`, `persist`, `destinations` and other keys fail closed | `project_cfg.rs:991` |
| `flagship` | partial — Flagship feature flags; per-entry keys `binding`, `app_id`, `remote` (`remote` accepted and ignored — no remote/local split; evaluation always uses the propagated app config). App definitions are tenant-level `flagshipApps` on the signed snapshot — standalone supplies them via `flagship.json` (bound-but-undeclared apps get an empty store); see [Flagship](bindings/flagship.md) | `project_cfg.rs:923` |
| `images` | partial — CF Images binding (`env.<binding>`); table keys `binding` and `remote` (`remote` accepted and ignored — no remote/local split; see [Images](bindings/images.md)). `info()` works via in-facade header sniffing; `transform()` is wired but returns 501 until the jSquash WASM engine is vendored. Renders `wrapped` → `cloudflare-internal:images-api` → the `facade-images` system worker | `project_cfg.rs` `images` arm |
| `k2` | partial — K2 durable event log produce binding; per-entry keys `binding`, `stream`, `remote` (`remote` accepted and ignored). `stream` is the user-side stream name (`[A-Za-z0-9_-]{1,256}`); the 32-hex `streamId` is derived server-side (`vrt-k2:{tenant}:{name}` standalone; server-minted row platform-side) — routing always uses the signed id. `env.<binding>.send(records)` resolves `{success:true}` or `{success:false,error:{code,retryable}}` and never throws (CF produce contract). Consumption is not a binding — it rides the platform subscription surface | `project_cfg.rs:1095` |
| `send_email` | supported — Email Sending binding; per-entry keys `name` (required), `destination_address`, `allowed_destination_addresses`, `allowed_sender_addresses` (`destination_address` and `allowed_destination_addresses` are mutually exclusive — both set fails closed; every address format-validated; unknown keys `VRT_CONFIG_UNSUPPORTED_FIELD`). `env.<name>.send(message)` validates the CF Workers Email Sending contract provider-side (from required, ≥1 of to/cc/bcc, subject required, ≤50 recipients, ≤32 attachments, ≤25MiB total) and resolves `{messageId}` on success or throws an `Error` carrying the CF `E_*` code in `.code`. Restrictions are signed into desired state and enforced on every send. Delivery is NOT self-hosted — the provider `[email]` config selects an adapter: `cloudflare`, `resend`, `smtp`, or `log` | `project_cfg.rs:1179` |
| `durable_objects` | supported — see [Durable Objects](bindings/durable-objects.md); `bindings[]` with `name`, `class_name`, `script_name` | `project_cfg.rs:1003` |
| `migrations` | supported — `tag`, `new_classes`, `new_sqlite_classes`, `renamed_classes` (`{from,to}`), `deleted_classes`; **`transferred_classes` rejected** | `project_cfg.rs:1062` |
| `triggers` | supported — `crons` only; see [Cron Triggers](bindings/cron-triggers.md) | `project_cfg.rs:1179` |
| `route` / `routes` | supported — pattern strings or `{ pattern }` → `routeHostnames`; `{ pattern, custom_domain = true }` → `customDomains` (bare FQDN claim, `enabled = false` drops it); `zone_name`/`zone_id`/`previews_enabled` accepted and ignored; `route` and `routes` are mutually exclusive (`VRT_CONFIG_INVALID`) | `project_cfg.rs:1591`, `route_to_hostname` `:555` |
| `assets` | supported — see [Static assets](bindings/static-assets.md); `directory`, `binding`, `html_handling`, `not_found_handling`, `run_worker_first` | `project_cfg.rs:1235` |
| `workflows` | supported — see [Workflows](bindings/workflows.md); `name`, `binding`, `class_name`, `script_name`, `limits.steps` | `project_cfg.rs:1269` |
| `tail_consumers` | supported — see [Tail Workers](bindings/tail-workers.md); `service` table or legacy string entries; distinct same-tenant already-deployed consumers, deduped, max 16; empty list valid | `project_cfg.rs:1352` |
| `streaming_tail_consumers` | supported — Streaming Tail Workers; per-entry key `service` only; same same-tenant resolution as `tail_consumers`, rendered as workerd `streamingTails` designators so the consumer's `tailStream()` receives the event stream; one channel per consumer | `project_cfg.rs` `streaming_tail_consumers` |
| `limits` | supported — see [Per-worker quotas](quotas.md); `cpu_ms` (1..300000) and `subrequests` (0..10000); other keys rejected | `project_cfg.rs:1393` |
| `dev` | supported (dev-only) — `ip` (loopback only; `localhost` normalizes to `127.0.0.1`, `::1` binds as `[::1]:port`), `port` (1..65535), `host` (extra route hostname registered for the session via `DeployOverrides.hostnames`; must satisfy the route hostname rule), `local_protocol = "http"` no-op. The deploy-facing projection accepts and drops the table, so dev-only keys never fail `vrt deploy`; `vrt dev` validates strictly at session start (`parse_dev_table`) where `inspector_*`, `upstream_protocol`, `remote`/`experimental_remote`, `enable_containers`, `container_engine`, `generate_types` are rejected | `project_cfg.rs` `parse_dev_table`, `dev.rs` `DevPorts::alloc` |
| `env` | supported — environment blocks resolved before normalization | `project_cfg.rs:1349`, env resolution earlier in the file |
| `rules` | supported — ordered `{ type, globs, fallthrough? }` module-typing rules (wrangler `Rule` shape, `additionalProperties: false`). Types: `ESModule`, `CommonJS`, `CompiledWasm`, `Text`, `Data`; `PythonModule`/`PythonRequirement` fail closed (no Python runtime). At pack time every gathered file under the rule root evaluates rules in order — user rules first, then the wrangler built-ins (`Text: **/*.txt,html,sql`, `Data: **/*.bin`, `CompiledWasm: **/*.wasm[?module]`), then the extension fallback; a matched non-`fallthrough` rule prunes every LATER rule of the same type (wrangler `parseRules`). The entrypoint is never retyped by a rule | `project_cfg.rs` `normalize_module_rules`; `worker_pkg.rs` `effective_module_rules` |
| `base_dir` | supported — the directory rule globs evaluate against (wrangler `moduleRoot`; default = the `main` directory). Files outside the root are not rule candidates and keep extension-fallback typing. Must be a clean relative dir inside the project (no absolute/traversal/`\\`/`:`) | `project_cfg.rs` `validate_base_dir`; `worker_pkg.rs` `module_root` |

## Accepted and ignored

Mirrors wrangler's own ignore list — present keys do not fail the deploy
but change nothing (`IGNORED_TOP_LEVEL`, `project_cfg.rs:31–57`):

`account_id`, `workers_dev`, `send_metrics`, `keep_vars`, `build`,
`alias`, `tsconfig`, `jsx_factory`, `jsx_fragment`,
`no_bundle`, `minify`, `keep_names`, `find_additional_modules`,
`preserve_file_names`, `define`, `upload_source_maps`,
`cwd`, `config`, `placement`.

`no_bundle`/`find_additional_modules`/`preserve_file_names` stay ignored
deliberately: the standalone pack ships the module table unbundled
already, so there is no bundler for those knobs to steer — `rules` only
chooses each module's uploaded type here, matching what wrangler's
`--no-bundle` path does with them.

Also type-checked then dropped inside supported blocks:

- `hyperdrive[].id` — accepted, never used as an identity
  (`project_cfg.rs:641`).
- `hyperdrive[].localConnectionString`, `hyperdrive[].remote` —
  dev-only keys, dropped by normalization.
- `kv_namespaces[].id`, `.preview_id`, `.remote` — `id` becomes the
  local namespace identifier; `preview_id`/`remote` are dropped.
- `d1_databases[].preview_database_id`, `.remote` — dropped.
- `analytics_engine_datasets[].remote` — accepted metadata; remote
  semantics unimplemented, dropped.
- `services[].entrypoint` — becomes the `{ service, entrypoint }` table
  form of `[bindings.service]`; the name is validated at pack
  (`worker_pkg.rs`; see [Service bindings](bindings/service-bindings.md)).
- `services[].props` — becomes the `{ service, props }` table form
  (or joins `entrypoint`); must be a JSON object of at most 8 KiB and is
  rendered into the signed workerd service designator as `ctx.props`.
- `routes[].custom_domain` — merges into the worker's wire `hostnames`
  (standalone: same as `vrt hostnames add`). Standalone has no DNS/TLS
  pipeline: the operator must point the domain at the vrt-host listener
  and terminate TLS in front (Caddy/nginx) or serve plain HTTP — same
  responsibility CF assigns to a zone it does not manage.

## Rejected

Any other top-level key → `VRT_CONFIG_UNSUPPORTED_FIELD`
(`project_cfg.rs:1355`):

```
[VRT_CONFIG_UNSUPPORTED_FIELD] <key> is not supported by the VRT runtime
```

That includes, non-exhaustively: `ai`, `vectorize`, `ai_search`,
`ai_search_namespaces`, `send_email`,
`mtls_certificates`, `pipelines`, `dispatch_namespaces`, `browser`,
`containers`, `wasm_modules`, `text_blobs`, `data_blobs`,
`version_metadata`, `secrets_store_secrets`,
`migrations_pattern`,
`logpush`, `site`, `preview_id` (top level),
`node_compat`. Inside `routes[]` objects, CF carries `custom_domain`,
`enabled`, `previews_enabled` and zone refs — all handled above;
wildcard `custom_domain` patterns (`*.example.com`) are rejected
(`VRT_CONFIG_UNSUPPORTED_FIELD`).

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
- 2026-11-21: `streaming_tail_consumers` is accepted (same shape and
  same-tenant rule, rendered as `streamingTails`); both tail lists share
  self-reference, dedupe and 16-cap validation, and a worker on both
  channels is refused rather than double-delivered.
- 2026-11-21 (r2): `[dev]` handling corrected against review — the
  shared normalize path now accepts-and-drops the table (mirroring the
  TS ignore list), so config `wrangler deploy` accepts (`ip =
  "0.0.0.0"`, `inspector_port`, …) can no longer fail `vrt deploy`.
  `vrt dev` re-validates it strictly at session start via
  `parse_dev_table` on the env-resolved config: loopback-only `ip`
  (IPv6 `::1` binds as a bracketed `SocketAddr`), `port`, and `host`
  validated against the route hostname rule. `dev.host` now rides
  `DeployOverrides.hostnames` into the deployed worker's routes
  instead of mutating a meta the deploy reloads from disk.
- 2026-11-21: `queues.producers[].delivery_delay` is accepted
  (0–86,400 seconds) as a queue-level default send delay, carried into
  the signed queue binding and applied by `facade-queue` when a send
  omits `delaySeconds`.
- 2026-11-21: `[observability]` review fixes — version identity is now
  derived from the canonical serialized `VersionRecord` instead of a
  hand-maintained field list, so observability (and any future
  version-scoped field) cannot be silently deduped away; head sampling
  is deterministic over real TraceItem fields instead of the
  never-present `traceId`/`spanId`; the table inherits into `[env.*]`
  (removed from `NON_INHERITABLE_ENV_KEYS`) and env blocks deep-merge
  onto it so a top-level `enabled = false` cannot flip back on; the
  deprecated rate alias now yields to `logs.head_sampling_rate`; and
  `VersionRecord` no longer uses `deny_unknown_fields`, so an older
  cell can read state written by a newer build after a rollback.
- 2026-11-20: `vrt deploy`, `vrt dev`, and `vrt doctor` accept `--env` to select Wrangler named environments.
- 2026-11-21: `[dev]` is consumed instead of ignored — `vrt dev` reads
  `ip`/`port` for the loopback ingress bind (`--listen` still wins, same
  flag-over-config order as wrangler) and registers `host` as a
  forwarded-`Host` hostname for the session. On the `vrt dev` entry
  `dev.ip` must be loopback: wrangler permits `0.0.0.0` to share a
  session on the LAN, but `vrt dev` never binds publicly, so a
  non-loopback value is `VRT_CONFIG_INVALID` there (same gate as
  `--listen`). Keys naming dev machinery vrt does not implement
  (`inspector_*`, `upstream_protocol`, `remote`, `experimental_remote`,
  `enable_containers`, `container_engine`, `generate_types`) fail closed
  as `VRT_CONFIG_UNSUPPORTED_FIELD` on that entry; `local_protocol` is
  accepted only as `"http"`, the sole mode served. `vrt deploy` accepts
  and drops the table (see the r2 entry above), matching wrangler where
  `[dev]` configures only `wrangler dev`.
- 2026-11-21: `flagship` is supported — `[[flagship]] {binding, app_id,
  remote}` (remote accepted-and-ignored) declares a version-scoped
  binding; the app record lives tenant-side in `flagshipApps` (flag
  edits propagate on the signed snapshot without redeploy) and renders
  as a `wrapped` binding over `cloudflare-internal:flagship` with the
  app config in a `config` json innerBinding. Standalone flag store:
  `flagship.json`; bound-but-undeclared apps synthesize an empty store.
  Admission requires the `flagship` cell capability.
- 2026-11-21: `k2` is supported — `[[k2]] {binding, stream, remote}`
  (remote accepted-and-ignored) declares a produce binding whose
  `send(records)` returns the non-throwing `{success}|{error{code,
  retryable}}` envelope. The stream name validates
  `[A-Za-z0-9_-]{1,256}`; the wire `streamId` (`^[0-9a-f]{32}$`) is
  derived/minted server-side — a client can never supply it. Storage
  is the vrt-sql K2 append log (per-stream namespace, ≤30-day TTL,
  subscription+lease consume surface on the provider side). Admission
  requires the `k2` cell capability.
- 2026-11-21: `send_email` is supported — `[[send_email]] {name,
  destination_address?, allowed_destination_addresses?,
  allowed_sender_addresses?}` declares a CF Email Sending binding.
  `destination_address` and `allowed_destination_addresses` are
  mutually exclusive at normalize time; all configured addresses are
  validated. The binding name is the resource key (no separate email
  resource row exists); the three restriction fields ride the signed
  desired state and the provider enforces them on every send — the
  signed `write` permission is required to send. Request validation
  follows the CF Workers Email Sending contract (`from` required, at
  least one of `to`/`cc`/`bcc`, `subject` required, ≤50 combined
  recipients, ≤32 attachments, ≤25MiB total, `{email,name}` address
  shapes, custom-header rules) and failures surface the CF `E_*`
  codes — `send()` throws an `Error` with `.code` set and resolves
  `{messageId}` on success. Delivery is pluggable, not self-hosted:
  the provider `[email]` config selects `cloudflare`, `resend`,
  `smtp`, or `log` (test/dev sink) adapters; upstream credentials
  live in provider config/env references, never in desired state;
  every upstream call is timeout-bounded and retryable failures map
  to HTTP 503 vs 502 for non-retryable. Admission requires the
  `email` cell capability.
- 2026-11-23: `images` is supported — `[images] {binding, remote}`
  (remote accepted-and-ignored) declares an Images binding rendered as
  `wrapped` → `cloudflare-internal:images-api` whose inner `fetcher`
  reaches the deny-routed `facade-images` system worker. `info()` runs
  in-facade header sniffing (PNG/JPEG/WebP/GIF/BMP); `transform()` is
  wired end to end but returns 501 `9490` until the jSquash WASM
  engine is vendored — it fails closed rather than passing the source
  bytes through. Source bounds: 64 MiB / 70 MP.
