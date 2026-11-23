# Vectorize binding

`env.<NAME>` is the Vectorize binding: `insert`/`upsert` store vectors,
`query` returns nearest matches with metric scores, `getByIds` /
`deleteByIds` address vectors by id, and `describe()` reports index
shape — the GA (v2) API surface from `vectorize.d.ts`.

**Status: Partial** — the full v2 method surface is implemented on an
exact-KNN backend (sqlite-vec `vec0` + f32 blob store). Differences
from Cloudflare are listed under *Known differences*; the wire
contract (`cloudflare-internal:vectorize-api`, `{code,error}` envelope)
matches upstream workerd's shim exactly.

## Wrangler configuration

```toml
[[vectorize]]
binding = "VECTORIZE"
index_name = "docs-index"
```

Native `vrt.toml` equivalent (the platform derives `indexId` from
`vrt-vx:{tenant}:{index_name}` — first 26 hex chars of sha256):

```toml
[bindings.vectorize.VECTORIZE]
index_name = "docs-index"
dimensions = 768
metric = "cosine"
```

`dimensions` (`1..=1536`) and `metric` (`cosine` / `euclidean` /
`dot-product`) are required — they are part of the signed index spec.
The index id is server-assigned; a user-supplied `index_id` in desired
state must be exactly 26 lowercase alphanumeric characters.

CF's `wrangler.toml` `[[vectorize]]` accepts `{binding, index_name}`
only — dimensions and metric live on the provisioned index there.
The VRT spec is signed at deploy and there is no separate index
provisioning surface, so `dimensions` and `metric` are required extra
keys here (a documented deviation). `remote` is accepted and ignored,
matching the local-dev precedent used by `kv`/`k2`/`images`.

## Using it

```js
export default {
  async fetch(req, env) {
    await env.VECTORIZE.insert([
      { id: "a", values: [1, 0, 0], metadata: { tier: "gold" }, namespace: "n1" },
      { id: "b", values: [0, 1, 0] },
    ]);
    const res = await env.VECTORIZE.query([1, 0, 0], {
      topK: 2,
      namespace: "n1",
      filter: { tier: { $eq: "gold" } },
      returnValues: true,
      returnMetadata: "all",
    });
    return Response.json(res);
  },
};
```

## Behaviour notes

- `query` accepts `{vector}` or `{vectorId}`; `topK` defaults to 5,
  `returnValues` to `false`, `returnMetadata` to `"none"`.
- Scores follow Cloudflare semantics: `cosine` returns the similarity
  in `[-1, 1]` (higher = closer, sorted desc), `euclidean` returns the
  L2 distance (sorted asc), `dot-product` returns the **negative** dot
  product (more negative = closer, sorted asc).
- `filter` supports `$eq` (implicit or explicit), `$ne`, `$lt`, `$lte`,
  `$gt`, `$gte`, `$in`, `$nin` over scalar metadata; `string[]`
  metadata matches a scalar op when any element does.
- `insert` rejects a duplicate id with a typed error; `upsert`
  replaces id, namespace, metadata and values atomically.
- `deleteByIds` is idempotent — unknown ids are ignored and a
  `mutationId` is still returned.
- Mutations are visible immediately; `describe().processedUpToMutation`
  tracks the local mutation sequence (see *Known differences*).

## Limits

| Limit | Value |
|---|---|
| Vectors per `insert`/`upsert` batch | 1000 |
| Vector id | 1–64 bytes |
| Metadata per vector | ≤ ~1 KB; values: string, number, boolean, `string[]` |
| Namespace | ≤ 63 bytes |
| Request body | 8 MiB |

## Where it runs

The binding renders as a workerd `wrapped` binding over
`cloudflare-internal:vectorize-api` — the upstream workerd shim,
shipped unmodified — whose inner `fetcher` service binding points at
the `facade-vectorize` system worker (`vrt-config/src/capnp.rs`,
`facade/facade-vectorize.ts`). `indexId` and `indexVersion = "v2"`
ride inner text bindings; signed props (bindingId, workerId,
`namespaceResourceId` = indexId, permissions) pin the index to the
deploy revision. The facade is deny-routed and only receives the ops
the shim posts.

The facade validates the route, strips user-controllable `x-vrt-*` /
`x-open-compute-*` headers, mints the provider request id, and
forwards to `/internal/bindings/v1/vectorize/<bindingId>/<op>` on the
provider socket — the same BINDING_BACKEND channel as d1/queue/k2.
Non-200 replies carry the `{code:number, error:string}` envelope the
upstream shim translates into `<OP>_ERROR (code = N)` errors.

## Backends

`vrt-provider`'s `vectorize` store: one SQLite file per index at
`<vectorize.dir>/<tenant>/<indexId>.vec.db`, registered with
sqlite-vec's `vec0` virtual table for the cosine/euclidean fast path
plus an f32-blob side table that owns ids, namespaces, metadata and
values. `dot-product` indexes rank via an exact Rust scan (vec0 has
no dot metric), as do all metadata- or namespace-filtered queries —
recall is always exact. `[vectorize] dir` on the provider config
enables the backend; absent → ops fail closed (`code: 3100`).

## Unsupported keys and error shape

- `[[vectorize]]` accepts `binding`, `index_name`, `dimensions`,
  `metric`, `remote` only — anything else →
  `VRT_CONFIG_UNSUPPORTED_FIELD` (`vectorize[i].<key>`).
- Missing/invalid `binding`, `index_name` (kebab-case, ≤64 B),
  `dimensions` (`1..=1536`) or `metric` → `VRT_CONFIG_INVALID`.
- A binding name colliding with another binding on the same worker →
  `DESIRED_DUPLICATE` (`bindingName`) at snapshot validation.
- At runtime, ops map to typed errors: bad route/body → `3000`,
  oversize body → `3001`, dims mismatch → `3010`, invalid id → `3012`,
  namespace too long → `3013`, duplicate id on `insert` → `3020`,
  unknown `vectorId` → `3030`, permission denied → `3004`, backend
  unavailable → `3100`, spec drift → `3101`.

## Examples and tests

- `snapshot-protocol/src/compute_desired.rs` — `VectorizeBinding`
  desired model + validation (index id charset, kebab-case name,
  dims/metric bounds).
- `vrt-local/src/project_cfg.rs` — `[[vectorize]]` normalize (key
  allowlist, dims/metric/index-name validation).
- `vrt-local/src/worker_pkg.rs` — `derive_index_id`
  (`vrt-vx:{tenant}:{index_name}` → 26-hex id).
- `vrt-config/tests/render.rs` —
  `vectorize_binding_renders_wrapped_vectorize_api` (wrapped module,
  `fetcher`/`indexId`/`indexVersion` inner bindings, signed props).
- `facade/test/vectorize.test.mjs` — route validation, identity-header
  strip + mint, `{code,error}` envelope pass-through (5 tests).
- `vrt-provider/src/vectorize.rs` — backend unit tests: cosine/euclidean
  ordering, negative-dot-product scores, namespace + metadata filters,
  insert/upsert/delete/getByIds round-trips, dims/spec-drift
  fail-closed (8 tests).

## Known differences from Cloudflare

- **Index deletion does not purge provider data.** The control plane
  deletes the index metadata only; the runtime has no Vectorize purge op,
  so the local `<tenant>/<indexId>.vec.db` can remain on disk. A
  re-created index always gets a new per-incarnation `indexId`, so stale
  vectors are never rebound to the new index. Reclaiming the file
  requires an explicit runtime/storage purge.
- **Exact KNN, not ANN.** Recall is 1.0 — a strict superset of the
  recall Cloudflare's approximate index returns. Result *order* and
  score values follow CF's metric semantics exactly.
- **`returnMetadata: "indexed"`** behaves as `"all"`: every stored
  metadata field is filterable. CF indexes only the fields declared on
  the index; we do not model metadata index declarations.
- **Synchronous mutation visibility.** CF applies mutations
  asynchronously (`processedUpToMutation` lags). Here a `mutationId`'s
  effects are visible to the next query — a superset of the contract.
  `describe().processedUpToDatetime` reports `0` (the platform tracks a
  mutation sequence, not a wall clock).
- **No index provisioning API surface in the binding.** The index is
  materialised on first use inside the tenant's signed spec; CF's
  `wrangler vectorize create` REST surface is a control-plane concern,
  not a binding behaviour.
- **`queryById` / `query(vectorId)`** on an unknown id returns a typed
  `code: 3030` error rather than an empty match set.
