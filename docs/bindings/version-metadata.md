# Version metadata

`env.<binding>` is the `WorkerVersionMetadata` object of the worker's
**active version** — `{id, tag, timestamp}`, as on Cloudflare. Rendered
as a `json` binding: the values are properties of the version
(`activeVersion.{versionId,tag,createdAt}`), never fields on the
binding itself.

**Status: Drop-in** — `[version_metadata]` normalizes and serves the
full `{id, tag, timestamp}` contract end to end; `id`/`timestamp`
derive from the platform version record, `tag` from the deploy
`--tag` flag.

## Wrangler configuration

```toml
[version_metadata]
binding = "CF_VERSION_METADATA"
```

Native `vrt.toml` equivalent — a single binding name:

```toml
[bindings]
versionMetadata = "CF_VERSION_METADATA"
```

`binding` is the only accepted field — `id`/`tag`/`timestamp` are not
config keys (they are version facts). Unknown keys fail closed
(`VRT_CONFIG_UNSUPPORTED_FIELD version_metadata.<key>`).

## Provisioning the value

Nothing to provision — the platform supplies the values at render:

- `id` — the active version's `versionId` (`vrt_<sha256:28>`,
  content-addressed over the version record: identical re-deploys
  keep the same id).
- `tag` — the deploy's `--tag` (`of deploy --tag v1`); `""` when
  untagged, matching `wrangler versions upload` without `--tag`.
- `timestamp` — ISO 8601 creation instant of the version record
  (deduped re-deploys report the FIRST upload's timestamp).

## Using it

```js
export default {
  async fetch(req, env) {
    const { id, tag, timestamp } = env.CF_VERSION_METADATA;
    env.WAE.writeDataPoint({
      indexes: [id],
      blobs: [tag, timestamp],
    });
    return new Response(`${id} ${tag} ${timestamp}`);
  },
};
```

## Behaviour notes

- The object is read-only metadata — `id`/`tag`/`timestamp` strings.
- A rollback moves the active-version pointer, so a worker reverted
  to version A reports A's id/tag/timestamp again.
- `tag` is part of version identity: two deploys identical but for
  `--tag` mint different version ids (the binding exposes it — same
  code with a different tag IS a behavioral difference).
- `timestamp` is NOT part of version identity (record metadata, not
  behavior) — a deduped re-deploy does not mint a new version.

## Limits

- One binding per worker (CF shape is a single table); names are
  free-form JS identifiers, `CF_VERSION_METADATA` is only convention.
- `tag` and `createdAt` are bounded (`activeVersion.tag` /
  `activeVersion.createdAt` ≤ the identifier bound at validation).

## Where it runs

In-worker — a `json` binding on the tenant worker itself, no service
hop, no shim module, no fork patch.

## Backends

No adapter — the values are facts of the signed desired state
(`activeVersion`), version-scoped and rollback-consistent by
construction.

## Unsupported keys and error shape

Only `binding` is accepted:

```
[VRT_CONFIG_UNSUPPORTED_FIELD] version_metadata.tag is not supported by the VRT runtime
```

`tag`/`timestamp` are deploy facts, not config — set the tag via
`of deploy --tag`.

## Examples and tests

- `vrt-config/tests/version_metadata_emit.rs` — json-binding emission
  with the worker's own version id, untagged `""` tag, absent-binding
  case.
- `vrt-local` `version_metadata_binding_into_pack_shape` — normalize
  + pack-shape coverage (`binding` only, unknown keys rejected).

## Known differences from Cloudflare

- Cloudflare mints a new version on every `versions upload` (even
  byte-identical); VRT dedups by content identity, so `id` stays
  stable across identical re-deploys (and `timestamp` keeps the first
  upload's instant). CF would report a fresh id/timestamp there.
- Cloudflare's version id is an opaque platform id; VRT's
  `vrt_<sha256:28>` is content-derived — format differs, semantics
  (immutable version identifier) match.
- CF tag sources: `wrangler versions upload --tag` / `--tag` on
  deploy; VRT: `of deploy --tag`.
