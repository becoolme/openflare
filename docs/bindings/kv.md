# KV

Workers KV namespaces bound to `env.*`, served by `vrt-provider` through
the vendored `KVNamespace` facade entrypoint.

**Status: Drop-in**

## Wrangler configuration

From `examples/standalone-wrangler/wrangler.jsonc`:

```jsonc
{
  "kv_namespaces": [
    // no `id` — standalone derives the namespace from the binding name
    { "binding": "CACHE" }
  ]
}
```

Native `vrt.toml` equivalent (`examples/standalone-kv-assets/vrt.toml`):

```toml
[bindings]
kv = { CACHE = "demo-ns" }
```

## Where it runs

- `facade-kv` — a system worker inside the tenant's workerd process,
  exposing the `KVNamespace` entrypoint (vendored from
  `third_party/open-compute`).
- `vrt-provider` — serves `/internal/bindings/v1/kv/{namespace}/{op}`
  (`op` ∈ `get`, `get-with-metadata`, `get-many`, `put`, `delete`,
  `list`), authorizes every call against the signed `compute.desired`
  snapshot, and owns the data on the configured backend.

## Backends

Selected in `vrt-provider.toml` (`vrt-provider/src/config.rs`); `[kv]`
and `[kvrocks]` are mutually exclusive — configuring both is rejected at
startup. With neither configured, KV operations fail closed with
`RESOURCE_UNAVAILABLE`.

| Backend | Config | Notes |
| --- | --- | --- |
| Filesystem | `[kv] dir = "…"` | Layout `<dir>/<tenant>/<namespace>/<sha256(key)>`; `0700` directories, atomic writes; same keyset ordering and pagination as the in-memory implementation. |
| Kvrocks | `[kvrocks] url` or `urlEnv` | Any Redis-protocol store; per-command and connect timeouts, one bounded read retry. |

An in-memory provider (`vrt-provider/src/kv/memory.rs`) exists for unit
and integration tests only — it is not selectable from the daemon
config.

## Known differences from Cloudflare

- `kv_namespaces[].id`, `preview_id` and `remote` are accepted. `id`
  becomes the namespace identifier in the local store — a Cloudflare
  resource id is never contacted. With no `id`, the namespace is derived
  from the binding name.
- `cacheTtl` on `get` / `getWithMetadata` (>= 30 seconds) drives a
  bounded process-local read cache in the KV facade. It is not the
  distributed Cloudflare edge cache: entries are per facade process,
  invalidated by writes through that process, and disappear on restart.
- API surface: `get`, `getWithMetadata`, bulk `get` (up to 100 keys),
  `put` (`expiration`, `expirationTtl`, `metadata`), `delete`, `list`
  with a signed, TTL'd cursor.
- Limits (`vrt-provider/src/limits.rs`): key ≤ 512 B, value ≤ 25 MiB,
  metadata ≤ 1024 B, `list` `limit` ≤ 1000, bulk get ≤ 100 keys and ≤
  25 MiB of response, `expirationTtl` ≥ 60 s, `expiration` ≥ now + 60 s.
- There is no Cloudflare-style hot read replica / edge cache; every op
  hits the configured backend.

## Unsupported keys and error shape

Any `kv_namespaces[]` key other than `binding`, `id`, `preview_id` and
`remote` fails the deploy:

```
[VRT_CONFIG_UNSUPPORTED_FIELD] kv_namespaces[0].<key> is not supported by the VRT runtime (kv_namespaces[0].<key>)
```

## Examples and tests

- `examples/standalone-kv-assets` — KV namespace in a `vrt.toml` project.
- `examples/standalone-wrangler` — `kv_namespaces` in `wrangler.jsonc`.
- `facade/test/kv.test.mjs` — facade KV contract tests.
- `vrt-provider/tests/protocol_kv.rs` — provider protocol tests.
- `vrt-provider/tests/fs_backends.rs` — filesystem backend tests.
- `vrt-provider/tests/kvrocks.rs` — Kvrocks backend tests.
- `vrt-provider/tests/kv_memory.rs` — shared keyset/pagination semantics tests.
