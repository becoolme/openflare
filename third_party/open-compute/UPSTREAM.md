# open-compute vendored subset — provenance

- Upstream: <https://github.com/elliothux/open-compute>
- Pinned commit: `8352f0cf9a43bcea1a443be3e6088706b1ea31b7`
- License: Apache-2.0 (`LICENSE` copied verbatim; the upstream root has no
  `NOTICE` file)
- Scope: only the KV and R2 transports plus the minimal JS closure they
  import. `config.capnp`, `host-extension.capnp`, the reference runtime and
  the conformance suite are not vendored.

## What is copied

Files keep their upstream `packages/runtime/src/` relative paths, placed under
`facade/`. Runtime import closure:

- `kv/transport.ts` (`KVNamespace`) -> `kv/native-adapter.ts`,
  `loader/shared.ts`, `cloudflare:workers`
- `r2/transport.ts` (`makeR2TransportBase`) -> `r2/native-adapter.ts`,
  `r2/validation.ts`, `bindings/private-transport.ts`, `cloudflare:workers`
- `kv/native-adapter.ts` -> `bindings/json-body.ts`
- `loader/shared.ts` -> `loader/python-snapshot.ts`, `loader/snapshot.ts`
  (carried only because `bindingError` / `INTERNAL_HEADERS` live there; the
  facade never calls the snapshot or Python helpers, the import graph just
  requires the modules to exist)
- `bindings/private-transport.ts` -> `loader/shared.ts`
- `bindings/json-body.ts` -> no dependencies

The `.d.ts` files are type-contract references (`import type` is erased at
runtime and they are not part of the worker module list). Transitive type
files they reference (for example `queues/protocol.d.ts`) are intentionally
not copied; this does not affect runtime behavior.

| File | sha256 |
| --- | --- |
| facade/bindings/json-body.ts | d825baf1890f95f520fa6b548b7f1487021e62e287e91670cf050b8bfdd862cd |
| facade/bindings/private-transport.ts | d3c81cd926da20e2e8529c72617a95dae8591102293a0d885773e8a6a52a2bf9 |
| facade/bindings/protocol.d.ts | 1d824d9b8f72f50d90f418c64812cfd2ea2632a625d71b83025af21dc3159f39 |
| facade/durable-objects/protocol.d.ts | 5668f0d87258be514a772742e58723479cbf5f5a7d5fa55ef031930fbdc2c2ec |
| facade/kv/native-adapter.ts | 7118df2fa817b6dfa7b6210dd04e9056af62753cc6d17f03b2389c2148117c42 |
| facade/kv/transport.ts | 7c8c6e7eb6d50b17bf70ee1e471d6a24005fe5990389a1349eea2c96b23ba1ee |
| facade/loader/protocol.d.ts | afc7d895ff354907348b6f09d2739a8bf7422df038979ed8b0c8de4f98b11222 |
| facade/loader/python-snapshot.ts | a69bc46cd34708e1b57848f60fde07d51b2a331f5c9ed5e5c23067a57bd8d292 |
| facade/loader/shared.ts | 0bcc1aef30a74df9d90237e4d2ef8276ab346bb0645207575746d9a0dd9feca5 |
| facade/loader/snapshot.ts | 61cb57e3b0295e97d078553a321a5c3c6c88aeda66cdfb228de62fcb9cdb885b |
| facade/r2/native-adapter.ts | f7798bc303b0cfe2076115064d9a53ff4139f5d0fb5b8de214a20a7aa058ceb0 |
| facade/r2/protocol.d.ts | 6ba944f8a412ecb0862cfc28f09aac58a3cacbd3366d32cfb4adf55f53303411 |
| facade/r2/transport.ts | 57807485b32d145439c8ff563a67c708d44833c7d686c241851d0b82ba26186a |
| facade/r2/validation.ts | 9ae80f5607a29493d0f776d2d243bad2b127569e3a74908d055e39d29a189d84 |

`LICENSE`: check with `shasum -a 256 third_party/open-compute/LICENSE`.

## Local modifications

One documented patch; everything else is byte-identical to upstream.

- `facade/r2/native-adapter.ts`, multipart `complete`: upstream strips the
  HTTP and custom metadata fields out of the `completeMultipartUpload`
  result (its comment notes Cloudflare's `complete()` omits them). The
  vendored copy instead passes the stored values through, because workerd
  parses the `complete()` result through the same non-list metadata path as
  `head`/`put` (`r2-multipart.c++`, `r2-bucket.c++`) and Cloudflare documents
  the result as a full `R2Object` with non-optional
  `httpMetadata`/`customMetadata` — stripping them would force callers to
  re-read the object. The rest of the file is unmodified.

All other platform adaptation (props -> identity headers, the
`BINDING_BACKEND` wrapper, the assets/pg transports, the module manifest)
lives in the repository's own `facade/` layer and does not edit files in
this directory.

## Updating

1. Check out the new upstream commit in a separate, read-only clone of
   open-compute.
2. Copy each file in the table over its counterpart. If imports were added or
   removed, recompute the import closure first
   (`facade/tools/gen-manifest.mjs` fails on any missing module).
3. Recompute the sha256 column (`shasum -a 256 facade/**/*.ts` in this
   directory) and update the pinned commit above. List any local patch in the
   "Local modifications" section above.
4. Regenerate `facade/{dist/,manifest.json}`
   (`cd facade && node tools/gen-manifest.mjs`), then run
   `cd facade && node --test`,
   `node ../contract/gen-facade-requests.mjs` (if the wire format changed) and
   `cargo test --workspace --offline`.
