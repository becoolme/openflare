# Version metadata

`env.<binding>` is an object describing the **serving version** of the
worker — `{ id, tag, timestamp }`. It exists so worker code can report its
own version in logs, error reports and health probes without the platform
baking the value into the bundle.

**Status: Partial** — `id` and `timestamp` match Cloudflare; `tag` is
always `""` because the standalone deploy surface has no version tags.

## Wrangler configuration

```toml
[version_metadata]
binding = "CF_VERSION_METADATA"
```

`binding` is the only accepted key.

```js
export default {
  async fetch(req, env) {
    // { id: "<version id>", tag: "", timestamp: "<epoch ms>" }
    return Response.json(env.CF_VERSION_METADATA);
  },
};
```

## Where it runs

- `vrt-local` stores the declared binding name and the version's creation
  time (epoch milliseconds) on the version record.
- At desired-state projection the **active** version synthesizes a workerd
  `json` binding whose value is
  `{ "id": "<version id>", "tag": "", "timestamp": "<created-at ms>" }` —
  computed from the serving version, not the deploy operation, so a
  rollback re-emits the reactivated version's own metadata.
- `vrt-config` renders it as `(name = "<binding>", json = "...")`; the
  value reaches `env` as a real object, not a JSON-encoded string.
- The serialized payload is bounded (16 KiB) and re-parsed before emit;
  deployments carrying it require the `versionMetadata` cell capability so
  a cell without `json`-binding support never receives one.

## Backends

Pure projection — nothing is stored per request, no provider is involved.
The three fields come from the version record the control plane already
persists.

## Known differences from Cloudflare

- `id` is the platform's version id, not a UUID — same "unique version
  identifier" contract, different format.
- `tag` is always `""`: Cloudflare fills it from version tags (created by
  `wrangler versions upload --tag` / deployments API), and the standalone
  deploy surface has no tag concept yet.
- `timestamp` is an epoch-millisecond **string** of the version's creation
  time, matching the documented Cloudflare shape.
- `message` (the Cloudflare-internal annotation on some versions) is not
  emitted — the object carries exactly `id`, `tag`, `timestamp`.

## Unsupported keys and error shape

- `[version_metadata]` accepts only `binding` — anything else →
  `VRT_CONFIG_UNSUPPORTED_FIELD` (`version_metadata.<key>`).
- Missing/non-string `binding` → `VRT_CONFIG_INVALID`
  (`version_metadata.binding`); an illegal binding name → the standard
  binding-name validation failure.
- A name colliding with another binding on the same worker →
  `DESIRED_DUPLICATE` at snapshot validation.
- A `json` binding value that fails to parse → `bindings.jsonValue`;
  over the 16 KiB bound → `jsonBindingBytes` capacity rejection; a cell
  not claiming the `versionMetadata` capability is never assigned one.

## Examples and tests

- `vrt-config/tests/tail_emit.rs` — `json = "{...}"` emit alongside the
  binding-name assertions.
- `vrt-local/src/state.rs` — the active version synthesizes
  `{id, tag: "", timestamp}` at `to_desired()`; rollback restores the
  reactivated version's values.
- `snapshot-protocol/src/compute_desired.rs` — `Json` binding validation
  vectors (parse bound, size bound, name uniqueness).
- `shared/vrt-project-config-vectors.json` — `[version_metadata]` parse
  vectors shared with the TypeScript project config.
