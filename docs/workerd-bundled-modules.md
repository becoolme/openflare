# workerd's bundled modules, and when we use them

workerd ships a growing set of **bundled modules** — ES module sources
compiled into the binary and addressed with
`Worker.Module.bundled = "/workerd/<name>.js"`. They are the local
implementations Miniflare/wrangler use; they are not Cloudflare's
production services.

This page records, per module, what it actually is, what we do instead,
and why. Two rules come out of it:

1. **Reuse when the module needs no backend.** If the bundled module is
   the whole implementation (no data plane, no identity), we run it
   verbatim — `secrets-store.js` is the only such case today.
2. **Reimplement when we need identity, durability or the control
   plane.** Every other bundled module stores data on a local disk owned
   by one Durable Object, with no auth boundary, no metering and no
   cross-cell story. Those are exactly the things a multi-tenant
   provider has to own.

Everything a bundled module "speaks" is a wire protocol we already
speak: `kvNamespace`, `r2Bucket`, `queue`, `cloudflare-internal:d1-api`
and the ratelimit RPC are implemented on the client side by workerd
itself, so the facade/provider only has to answer the same JSON/RPC
requests the bundled store would.

## Matrix

| Bundled module | What it is | What we run | Why not the bundled one |
| --- | --- | --- | --- |
| [`/workerd/kv.js`](#kv) | `KVNamespace` DO + local disk (optional cluster shard/blob mode) | `facade-kv` → provider → Kvrocks / fs | its props are `{namespace}` only — no signed identity, quota or metering; a DO disk is not a durable multi-cell store |
| [`/workerd/r2.js`](#r2) | Objects as keys in a `bucket` service, no index | native `r2Bucket` → `facade-r2` → provider → S3 / fs | no conditional writes, multipart semantics, checksums or per-tenant quota; bucket service is a local store |
| [`/workerd/queues.js`](#queues) | `QueueShard` DO + static consumer table | native `queue` → `facade-queue` + qconsumer → provider → `vrt-sql` | consumers are config-static; no control-plane registration, cross-cell dispatch, DLQ accounting or dedupe |
| [`/workerd/d1.js`](#d1) | `D1Database` DO + SQLite | `cloudflare-internal:d1-api` → `facade-d1` → provider → `vrt-sql` | single copy, fake bookmarks, no migrations, no metering; see [D1](bindings/d1.md) |
| [`/workerd/ratelimit.js`](#rate-limiting) | in-memory fixed-window counters | `facade-ratelimit` (same semantics) | props cannot carry tenant identity; scope must come from host-injected props |
| [`/workerd/cron.js`](#cron-triggers) | `Schedule` DO alarm, at-least-once | `cell-controller`/`vrt serve` cron runner + `cron-dispatcher` | schedule must come from the signed desired snapshot; delivery is at-most-once per minute slot with a claim log |
| [`/workerd/workflows.js`](#workflows) | Workflows engine as a DO | **vendored** Cloudflare engine (same lineage) | same code, but we pin it ourselves and host one engine per (defining worker, workflow) on that worker's disk |
| [`/workerd/secrets-store.js`](#secrets-store) | `env:<name>` lookup, no storage | **the bundled module, verbatim** | nothing to add: no backend, no identity |

## Per-module notes

### KV

`kv.js` keeps one Durable Object per namespace with `enableSql` and a
local disk (`SHARD`, optional `BLOBS`/`BLOB_CACHE`/`CLUSTERED` for a
cluster config). It has no authentication: whoever can reach the
service can read the namespace.

A tenant KV binding in VRT must carry a **signed** identity
(`bindingId`, `instanceId`, `permissions`, `resourceSpecGeneration`) so
the provider can authorise and meter the call. workerd's native
`kvNamespace` binding only carries `{namespace}` in its props, so we use
a plain `service` binding to `facade-kv` and reimplement the
`KVNamespace` client surface there (vendored from open-compute), with
Kvrocks as the durable backend.

### R2

`r2.js` stores each object as a key in a `bucket` service and keeps
metadata in a `workerd-r2` user-metadata blob. There is no index, no
conditional-write semantics, no multipart abort/complete bookkeeping, no
checksums and no per-tenant accounting.

We keep workerd's native `r2Bucket` client (so `R2ObjectBody`,
`httpMetadata` and stream bodies behave exactly like Cloudflare) and
point it at `facade-r2`, whose provider implements the full R2 wire on
S3 or fs — including `onlyIf`, multipart and checksums.

### Queues

`queues.js` is a `QueueShard` DO plus a `CONSUMERS` JSON table baked
into the config (production defaults: batch 10, 5 s, 3 retries, …).
Consumers therefore cannot change per deployment without a full config
render, and there is no cross-cell dispatcher.

VRT registers consumers per version in the control plane and delivers
through a dispatcher onto `vrt-sql`, with DLQ, retry-delay and
concurrency accounting. We still use workerd's native `queue` client.
See [Queues](bindings/queues.md).

### D1

`d1.js` is one SQLite-backed DO per database. Its own header says it:
one copy, one object's throughput, SQLite's limits rather than D1's,
bookmarks that cost no write and satisfy every session, no migrations.
See [D1](bindings/d1.md) for the full comparison with `vrt-sql`.

### Rate limiting

`ratelimit.js` is deliberately simple: in-memory counts per namespace,
forgotten on restart, `TEST_CLOCK` for tests. Our `facade-ratelimit` has
the same semantics — the difference is that the counter scope is derived
from host-injected props (`instanceId + workerId + namespaceId`), which
the bundled module's `{namespace, limit, period}` props cannot express.
See [Rate limiting](bindings/rate-limiting.md).

### Cron triggers

`cron.js` arms each schedule as a `Schedule` DO alarm, so an occurrence
runs once no matter how many cluster members load the config — but it is
at-least-once, and the schedule lives in the rendered config.

VRT takes schedules from the signed `compute.desired` snapshot, claims
each `(workerId, versionId, scheduleId, scheduledTime)` in a durable log
(at-most-once per minute slot), dedupes again on an idempotency header,
and wakes hibernated tenants. See [Cron Triggers](bindings/cron-triggers.md).

### Workflows

This is the one we *do* reuse — but vendored, not `bundled`. The engine
is the same Cloudflare code (miniflare lineage, `packages/
workflows-shared`), committed with SHA-256 pinning and an import-closure
test. Vendoring lets us run one engine service per (defining worker,
workflow) on that worker's disk, keep the engine version pinned
independently of the workerd pin, and register the `zod.worker.js`
extension module the engine imports. See [Workflows](bindings/workflows.md) and
`vrt-config/system-workers/workflows/VENDOR.md`.

### Secrets Store

`secrets-store.js` stores nothing: `get()` reads
`this.env["env:" + secret_name]`, which `cli/wrangler.rs` binds
`fromEnvironment`. There is no backend and no identity to add, so we run
the bundled module verbatim in a per-worker `secrets-<workerId>` service.
See [Secrets Store](bindings/secrets-store.md).

## Keeping this current

The list above mirrors `findBundled()` in workerd's
`src/workerd/server/workerd-api.c++` (and the `bundled @11` field docs in
`workerd.capnp`). When workerd adds a module:

1. check whether it needs a backend or identity — if not, run it
   verbatim;
2. if it does, add a row here and a "Why not workerd's bundled `X`"
   section on the binding's page;
3. keep the conformance reference runner (`conformance/reference.mjs`)
   covering the modules whose semantics we reimplement, so drift shows
   up as a diff rather than a bug report.
