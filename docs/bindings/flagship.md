# Flagship

`env.<binding>` is a Cloudflare Flagship feature-flag client — the
worker calls the typed `get*`/`get*Details` evaluation methods and the
flags resolve **inside the isolate** from the app config propagated on
the signed desired state (no central-service round trip, same
evaluation locality as Cloudflare).

**Status: Partial** — `[[flagship]]` bindings, typed getters/details,
targeting rules, percentage rollouts and a standalone flag store work;
there is no hosted flag-management UI yet — standalone apps define
flags in `flagship.json`, platform apps come from the control plane.

## Wrangler configuration

```toml
[[flagship]]
binding = "FLAGS"
app_id  = "my-app"
# remote = true        # accepted and ignored — VRT has no remote/local
                       # split; evaluation always uses the propagated
                       # config (same treatment as `remote` on other
                       # bindings)
```

One worker may bind several Flagship apps — repeat `[[flagship]]` per
app. `binding` must be a JS identifier; `app_id` is capped at 128
chars; any other key fails closed.

## Using it

```js
export default {
  async fetch(req, env) {
    const on = await env.FLAGS.getBoolean("new-checkout", false, {
      userId: "u-1",
      plan: "pro",
    });
    const detail = await env.FLAGS.getStringDetails("theme", "light", {
      userId: "u-1",
    });
    // detail: { flagKey, value, variant?, reason, errorCode?, errorMessage? }
    return new Response(on ? "new" : detail.value);
  },
};
```

`get(flagKey, context?, default?)`, `getBoolean/getString/getNumber/
getJson` and their `...Details` counterparts follow the Cloudflare
contract:

- A **missing flag** resolves to the caller-supplied default
  (`reason: "DEFAULT"` when a default was given, `FLAG_NOT_FOUND`
  otherwise).
- **Typed getters** answer the default with
  `errorCode: "TYPE_MISMATCH"` when the evaluated variant is the wrong
  type.
- **Details** carry `flagKey`, `value`, optional `variant`, `reason`
  (`TARGETING_MATCH`, `SPLIT`, `STATIC`, `DEFAULT`, `DISABLED`,
  `CACHED`, `ERROR`) and on error `errorCode` (`TYPE_MISMATCH`,
  `FLAG_NOT_FOUND`, `INVALID_CONTEXT`, `PARSE_ERROR`, `GENERAL`) +
  `errorMessage`.
- **Evaluation context** accepts recursive JSON values plus `Date`
  (normalized to ISO 8601). Non-finite numbers, unsupported objects,
  cycles and over-deep structures are rejected: details methods answer
  the supplied default with `reason: "ERROR"` /
  `errorCode: "INVALID_CONTEXT"`.
- **Targeting** supports dot-path attributes, `all`/`any` condition
  groups and percentage rollouts. Rollout buckets are deterministic
  (FNV-1a over app+flag+identity), so the same request always lands in
  the same bucket — and different flags never share a bucket.

## Where it runs

Each `flagship` binding renders as a workerd `wrapped` binding over the
`cloudflare-internal:flagship` internal TypeScript module (the same
mechanism as Workflows/D1); the app config rides a `config` JSON
innerBinding:

```capnp
(name = "FLAGS", wrapped = (
  moduleName = "cloudflare-internal:flagship",
  innerBindings = [(name = "config", json = "<app config>")],
))
```

The module's default export builds the binding from `env.config`, so
evaluation is pure in-isolate work — no socket, no fetch, matching
Cloudflare's local-evaluation contract. Flag apps are **tenant-level**
data on the signed desired doc (`tenants[].flagshipApps`), not version
config — a flag edit republishes the snapshot without minting a new
Worker version, matching CF's "update propagates without redeploy"
contract. A binding naming an app absent from the tenant's set fails
closed at desired validation; a render-time double-check excludes the
offending worker (per-worker isolation) rather than silently
evaluating defaults.

## Backends

- **Standalone**: `<worker_dir>/flagship.json` is the local flag store
  (the analog of `wrangler dev`'s local store). Shape:
  `{ "<appId>": { "accountTag"?, "flags": { ... } } }` (≤ 256 KiB).
  Entries merge per-app into tenant state on deploy — a file-provided
  app overwrites its same-id entry, and a bound `app_id` absent from
  the file synthesizes an empty `{"flags":{}}` config (evaluations
  return call-site defaults, like CF's empty local store). A deploy
  that drops the file keeps previously stored apps — the store is
  tenant data, never implicitly deleted.
- **Platform**: apps come from the control plane's Flagship store and
  ride the signed snapshot; flag updates trigger a desired republish
  to every bound worker's cell.

## Known differences from Cloudflare

Identical to Cloudflare (same evaluation contract, verified against
the documented binding surface):

- Context validation: non-finite numbers, cycles, unsupported objects
  and depth/size overflow are `INVALID_CONTEXT` — details methods
  answer the default, never throw.
- Valid `Date` context values normalize to ISO 8601 strings before
  evaluation.
- `get(key, ctx)` with no default throws/`FLAG_NOT_FOUND` on a missing
  flag; with a default it resolves the default (`reason: "DEFAULT"`).
- `remote` is a local-dev-only knob on Cloudflare; VRT accepts and
  ignores it (same as AE/KV `remote`).

Deliberate differences:

- Flag content is operator/control-plane supplied (`flagship.json`
  standalone, Flagship store platform-side); Cloudflare's hosted
  app/flag management UI has no analog yet.
- Bucket seed includes the flag key (an app-level bucket would alias
  unrelated flags) — deterministic per (app, flag, identity).

## Unsupported keys and error shape

- `[[flagship]]` accepts exactly `binding`, `app_id`, `remote` —
  anything else is `VRT_CONFIG_UNSUPPORTED_FIELD` naming
  `flagship[<i>].<key>`; a missing/non-string `binding`/`app_id`, an
  `app_id` over 128 chars or a non-boolean `remote` is
  `VRT_CONFIG_INVALID` naming the field.
- A malformed `flagship.json` (non-object, oversized, non-model flag)
  fails the deploy as `flagship.json`-named errors before anything
  publishes.
- A `flagship` binding whose `appId` is absent from the tenant's
  `flagshipApps` fails desired validation (`DESIRED_FLAGSHIP_APP`);
  render re-checks and excludes the worker with
  `CONFIG_UNRESOLVED_REFERENCE`.

## Examples and tests

```toml
[[flagship]]
binding = "FLAGS"
app_id  = "storefront"
```

```json
// flagship.json — standalone flag store
{
  "storefront": {
    "flags": {
      "new-checkout": {
        "enabled": true,
        "type": "boolean",
        "variants": { "on": true, "off": false },
        "defaultVariant": "off",
        "rules": [
          { "conditions": { "all": [{ "attribute": "plan", "operator": "equals", "value": "pro" }] }, "variant": "on" }
        ]
      }
    }
  }
}
```

Coverage:

- `vrt-local` — `[[flagship]]` normalization + fail-closed keys
  (`project_cfg` pack-shape test), shared golden vectors
  (`shared/vrt-project-config-vectors.json`), end-to-end
  deploy/merge/synthetic-empty/fail-closed (`local_ops` flagship
  suite).
- `snapshot-protocol` — wire shape, appId scoping, duplicate and
  malformed-config rejection.
- `vrt-config` — `flagship_emit` covers wrapped-binding emit, multi-app
  binding, and worker exclusion on an unresolvable `appId`.
- `workerd-fork` — `flagship-api-test.wd-test` drives the internal
  module across raw/typed/details getters, contexts, operators, groups
  and rollouts.
