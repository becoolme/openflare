# R2 binding

How a tenant worker's `env.BUCKET` (Cloudflare R2 compatible) reaches object
storage.

## Path

```
tenant worker -- r2Bucket binding --> facade-r2 (R2Transport.fetch) --> native adapter
   --> R2Transport methods --> BINDING_BACKEND --> vrt-provider /r2/* --> object store
```

- The tenant config renders the binding as a native workerd `r2Bucket`
  binding (`vrt-config/src/capnp.rs`, `emit_r2_bucket_binding`). workerd then
  provides the tenant-facing `R2Bucket` / `R2ObjectBody` classes and option
  parsing, and speaks its native R2 wire (GET for head/get/list, PUT for
  put/delete/multipart, `cf-r2-request` header) to the facade.
- The facade's `fetch()` translates that wire to the strict typed
  `R2Transport` methods. Those methods are an internal contract for the
  adapter and must never be handed to a tenant directly.
- KV and assets bindings stay plain `service` bindings; they have no native
  workerd binding class.

## Invariants

- R2 bindings MUST be emitted as `r2Bucket`, never `service`. A `service`
  binding exposes the raw transport: `put("k", "text", opts)` fails with
  `R2_INVALID_OPTIONS` (stream-only bodies), `get` returns `{meta, body}`
  with no `text()`, and `delete("k")` posts `{keys: "k"}`, which the provider
  rejects as `BINDING_PROTOCOL_ERROR`.
- Invalid option shapes must still fail closed (covered by a test).

## Tests

- `facade/test/r2.test.mjs` calls transport methods directly (no workerd).
- `facade/test/r2-workerd.test.mjs` runs a real workerd with an `r2Bucket`
  binding against an in-memory strict fake of the provider wire. It skips
  when no workerd binary is available (`WORKERD_BIN` or `workerd` on PATH).
  It does not exercise the real Rust provider or the object store, nor
  multipart uploads.
- `vrt-config/tests/golden/tenant.capnp` pins the rendered binding form.

## Changelog

- 2026-11-19: Render R2 bindings as native `r2Bucket` instead of a plain
  service binding. Fixes tenant `put` with httpMetadata/customMetadata
  (`R2_INVALID_OPTIONS`), `get` results without `text()`/`json()`, and
  `delete` failing with `BINDING_PROTOCOL_ERROR`. Added the real-workerd
  regression test and updated the golden render and digest.
