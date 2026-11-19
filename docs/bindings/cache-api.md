# Cache API

`caches.default` and `caches.open` on a per-tenant, in-memory cache
service built into `vrt-host`.

**Status: Partial**

## Wrangler configuration

No wrangler key — `caches.*` needs no declaration. The service must be
enabled host-side in `vrt-host.toml`:

```toml
[cache]
# maxEntryBytes = 8388608        # 8 MiB per stored entry
# tenantBytes = 134217728        # 128 MiB per tenant
# globalBytes = 536870912        # 512 MiB across the service
# globalInflightBytes = 67108864 # 64 MiB in-flight PUT bodies
# tenantInflightBytes = 33554432 # 32 MiB in-flight PUT bodies per tenant
```

Absent `[cache]` ⇒ `caches.*` throws `No Cache was configured` inside
the worker.

## Where it runs

`vrt-host` binds workerd's `cacheApiOutbound` designator to a per-tenant
unix socket under `<workDir>/cache/` (`vrt-host/src/cache.rs`). Tenant
identity is bound at listener setup — `CF-Cache-Namespace` only
subdivides the owning tenant's key space.

## Backends

In-memory LRU only, per host and per tenant: `match` → `GET` (+ `CF-
Cache-Status: HIT` ⇒ hit, `MISS`/`EXPIRED`/absent ⇒ `undefined`), `put`
→ `PUT` with a serialized response body (`204` stored / `413` too
large), `delete` → `PURGE` (`200` → `true`, `404` → `false`).

## Known differences from Cloudflare

- **Not persistent and not shared.** Entries live in the host process;
  a restart or a different host starts cold. There is no global
  Cloudflare-edge cache to inherit.
- `match` options (`ignoreMethod`, `ignoreSearch`, `ignoreVary`,
  `cacheName`) are unimplemented in the pinned workerd version — only
  GET requests reach the service (`vrt-host/src/cache.rs` header
  contract).
- Capacity is bounded: per-entry, per-tenant and global byte caps plus
  in-flight PUT byte caps (`vrt-host.toml` `[cache]`); oversized
  entries are simply not stored.
- Internal failures degrade to miss/no-store — the service never turns
  a miss path into a 5xx or a fake hit.

## Unsupported keys and error shape

Not a wrangler binding — nothing to reject. Host `[cache]` values are
typed and range-checked at config load; unknown keys fail closed.

## Examples and tests

- `examples/standalone-cache` — `caches.default` and `caches.open`
  against the per-tenant service.
- `vrt-host/tests/cache_smoke.rs` — end-to-end smoke test (skips without
  a real workerd).
- `vrt-host/src/cache.rs` — service implementation and wire contract.
