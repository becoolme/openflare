# Durable Objects

Durable Object namespaces and classes declared in wrangler form, with
SQLite storage on workerd's `localDisk` path inside the tenant workerd.

**Status: Partial**

## Wrangler configuration

From `examples/standalone-do/wrangler.toml`:

```toml
[durable_objects]
bindings = [{ name = "COUNTER", class_name = "Counter" }]

[[migrations]]
tag = "v1"
new_sqlite_classes = ["Counter"]
```

A binding in another worker of the same tenant is allowed via
`script_name` — the target worker must already be deployed (it is
resolved to a worker id at pack time, fail closed on a miss).

## Where it runs

- The namespace stub (`env.COUNTER.idFromName(...).get(...)`) is a native
  workerd `durableObjectNamespace` binding; the actor runs inside the
  declaring worker's isolate.
- Storage is `localDisk` SQLite under the host's `doStorageDir`:
  `<dir>/<tenant>/<workerId>/<uniqueKey>/`. `uniqueKey` is server-minted
  at deploy (HMAC over tenant + worker + class under the deployment
  signing key material) — never client input.
- `vrt-host` owns DO lifecycle: placement leases and snapshots
  (`[placement]`, `[doSnapshots]` in `vrt-host.toml`), WAL machinery
  (`vrt-host/src/do_wal.rs`), and admin endpoints `of do-snapshot` /
  `of do-migrate`.
- Storage state is external to the bundle: a worker can be restarted,
  rescheduled or redeployed without losing object data.
- Alarms are durable object metadata, not process state:
  `storage.setAlarm()` persists the armed time, so an alarm armed before
  a runtime restart is still delivered by the new process and
  `getAlarm()` keeps reporting it. Restarting `vrt-host` (which
  supervises workerd) in place does not drop a pending alarm;
  `conformance/cases/durable-objects/do-alarm-restart` proves this
  end-to-end.

## Backends

One storage mechanism: `localDisk` SQLite managed by `vrt-host`
(`doStorageDir`, required whenever a tenant declares DO classes or
workflows — its absence fails the tenant render). `new_classes` (the
KV-backed class kind on Cloudflare) maps to the same SQLite store with
`enableSql = false`, i.e. the `state.storage.sql` surface is not
exposed; `new_sqlite_classes` sets `enableSql = true`.

## Known differences from Cloudflare

- `transferred_classes` is **rejected at deploy time**. Standalone
  derives a class's storage identity from `(tenant, worker, class)`, so
  a cross-worker transfer would silently point it at an empty directory;
  it fails closed with `VRT_CONFIG_UNSUPPORTED_FIELD` and an explanatory
  message (`vrt-local/src/project_cfg.rs`).
- `new_classes` objects run on the same SQLite store as
  `new_sqlite_classes`; they just lack `state.storage.sql`. On
  Cloudflare they use a distinct KV-backed store.
- **DO hibernation is gated.** A DO tenant may sleep only when the cell
  controller pushes a fresh safe alarm gate (`POST /v1/do-alarm/gate`) backed by
  the persistent alarm index. Due alarms wake the tenant through
  `/v1/do-alarm/wake`; workerd delivers the persisted `_cf_ALARM` on startup.
  Without a gate (older controller, failed scan, unknown class) the tenant
  stays resident. Workflow-declaring tenants still never hibernate.
- Cloudflare's WebSocket-hibernation eviction is not emulated: an open
  Durable Object WebSocket session holds the tenant's in-flight guard,
  keeping the tenant awake until it closes (see
  [`docs/ingress-websocket.md`](../ingress-websocket.md)).
- `script_name` may only name an already-deployed worker **of the same
  tenant**; anything else fails closed at pack time.
- `migrations` are evaluated at deploy into the version's class set;
  `renamed_classes` carries the storage identity (`uniqueKey`) over so
  data survives the rename, `deleted_classes` removes the declaration.

## Unsupported keys and error shape

- `durable_objects` may only contain `bindings`; any other key (e.g.
  `durable_objects.something`) → `VRT_CONFIG_UNSUPPORTED_FIELD`.
- Per binding: only `name`, `class_name`, `script_name` —
  `durable_objects.bindings[0].<key>` → `VRT_CONFIG_UNSUPPORTED_FIELD`.
- Per `[[migrations]]` entry: `tag`, `new_classes`,
  `new_sqlite_classes`, `renamed_classes` (`{from, to}`),
  `deleted_classes`. `transferred_classes` →
  `VRT_CONFIG_UNSUPPORTED_FIELD` with a reason string; any other key →
  `VRT_CONFIG_UNSUPPORTED_FIELD`.

## Examples and tests

- `examples/standalone-do` — `durable_objects.bindings` + a SQLite
  `[[migrations]]` entry, storage + alarm over `localDisk`.
- `conformance/cases/durable-objects/do-alarm-restart` — arms a
  self re-arming alarm, restarts `vrt-host` in place through the
  conformance runner's `restart` step, and asserts that the next alarm
  fires in the new process while pre-restart storage is intact.
- `vrt-host/tests/do_wal.rs`, `vrt-host/tests/do_transfer.rs` — WAL and
  transfer-path tests.
- `vrt-host/tests/do_w1r4_workerd.rs` — real-workerd DO tests (skip
  without a binary).
- `vrt-host/tests/s2z_hibernate_workerd.rs` — scale-to-zero / hibernation
  tests (skip without a binary).
- `shared/vrt-project-config-vectors.json` — golden vectors covering
  `durable_objects` and `migrations` normalization.

## Changelog

- 2026-11-20: Alarm persistence across a runtime process restart is now
  covered by the conformance suite (`do-alarm-restart`): a pending alarm
  is delivered by the new `vrt-host`/workerd process and pre-restart
  object storage is still present. No behavior change.
