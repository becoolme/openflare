# Tail Workers

`tail_consumers` attaches a second Worker as a Tail Worker: after every
producer invocation workerd delivers a batch of `TailItem` events to the
consumer's `tail(events, env, ctx)` handler — `scriptName`, `outcome`,
`eventTimestamp`, `event`, `logs`, `exceptions` and diagnostic-channel
events, exactly the Cloudflare `TailEvent` shape.

**Status: Partial**

## Wrangler configuration

```toml
[[tail_consumers]]
service = "tail-worker"
```

JSON object form and the legacy string array are both accepted:

```jsonc
{
  "tail_consumers": [
    { "service": "tail-worker" },
    "audit-worker"
  ]
}
```

The native `vrt.toml` equivalent is:

```toml
[[tailConsumers]]
service = "tail-worker"
```

`service` names a **different, already-deployed Worker in the same
tenant**. It is resolved to the consumer's stable worker id at pack time
(`vrt-local/src/worker_pkg.rs`); an unknown name, a self-reference, a
cross-tenant target or an entry over the 16-consumer cap fails the
deploy before any generation is written. Duplicate names collapse to the
first occurrence and declaration order is preserved. An empty list is
valid and means "no tail consumers". Unknown entry keys (for example
`environment`) fail closed with `VRT_CONFIG_UNSUPPORTED_FIELD`; the
consumer is not created implicitly, so deploy it first. As in Wrangler,
`tail_consumers` is non-inheritable: an `env.<name>` block replaces the
top-level list wholesale.

## Where it runs

The producer's rendered workerd `Worker.tails` list
(`vrt-config/src/capnp.rs`) carries one service designator per
declared consumer, pointing at that consumer's `worker-<workerId>`
service in the **same tenant workerd process**. Events never cross a
tenant boundary and never leave the process. The consumer must export a
`tail()` handler; as on Cloudflare, the handler runs after the producer
invocation finishes and can only use `ctx.waitUntil()` for asynchronous
work.

At deploy time the target is validated three times: wrangler
normalization (`vrt-local/src/project_cfg.rs`), pack-time name
resolution, and snapshot/render validation against the tenant's worker
ids. That layering means a dangling designator can never reach workerd,
where it would otherwise fail the whole config load and take every
sibling worker down with it.

## Coexistence with the platform log collector

OpenFlare already attaches an internal `vrt-tail-<workerId>` service per
tenant Worker to collect logs into `vrt-logsink`. User consumers share
the same `tails` list: the internal collector is always emitted first,
followed by the declared consumers in stable order. Giving a Worker a
user tail **never** replaces or disables platform log collection, and a
Worker with no `tail_consumers` renders byte-identically to before this
feature existed.

## Backends

There is no platform-side queue: events flow producer isolate → consumer
isolate inside the tenant's workerd process. The platform's internal
per-worker log tail (`vrt-tail-<worker>`) is just another entry in the same
`tails` list and is unaffected by user consumers. The platform collector
honours `[observability]`: when the worker's `enabled` or
`logs.enabled` is `false` no collector is rendered at all (nothing is
collected); otherwise an `OBS` text binding carries
`{"headSamplingRate":<float>, "invocationLogs":<bool>}` and the embedded
collector samples per invocation — a dropped invocation keeps none of its
log/exception/outcome records (deterministic FNV-1a over the TraceItem's
`scriptName` + `eventTimestamp` + `durableObjectId` — the pinned workerd
exposes no `traceId`/`spanId` on `tail()` events, so a re-delivered event
gets the same verdict), and `invocation_logs=false` drops
only the outcome record. The wire carries the rate as integer ppm
(`headSamplingRatePpm`, 0..1_000_000) because the signed snapshot's
canonical JSON accepts safe integers only.

## Known differences from Cloudflare

- **Best effort only.** Tail events are not retried or persisted by the
  runtime. If the consumer fails to load, is mid-deploy or the process
  is shutting down, events are dropped. Do not use Tail Workers for
  billing, audit trails or any record that must be complete.
- **No streaming tail consumers.** `streamingTails` / streaming
  `tailStream()` delivery is not supported; only the batched
  `tail(events)` contract is rendered.
- **Same tenant only.** The consumer must be a Worker of the deploying
  team and must already exist. Cloudflare can attach any Worker of the
  account; OpenFlare resolves inside the tenant to keep the signed
  snapshot self-contained.
- **No self-tail.** A Worker cannot list itself as a consumer. CF
  documents Tail Workers as watching *other* Workers, and the runtime
  offers no documented recursion bound for a self-tail.
- **No environment/namespace scoping.** `service` is the only accepted
  key; the API-level `environment`/`namespace` selectors are rejected.
- **`scriptName` is usually null.** workerd's standalone config has no
  per-script name separate from the service, so `TailItem.scriptName`
  is null in a self-hosted deployment (the field is still present).
  Identify the producer by the configured consumer/service instead of
  relying on `scriptName`.
- Platform log collection is an implementation detail and is not
  configurable through `tail_consumers`.

## Unsupported keys and error shape

| Input | Result |
| --- | --- |
| `tail_consumers` not an array | `VRT_CONFIG_INVALID` |
| Entry not a string or `{ service }` table | `VRT_CONFIG_INVALID` |
| Missing/empty `service`, or unknown entry key | `VRT_CONFIG_INVALID` / `VRT_CONFIG_UNSUPPORTED_FIELD` |
| More than 16 consumers | `VRT_CONFIG_INVALID` |
| `service` not a deployed same-tenant Worker | deploy-time error naming `tail_consumers service target` |
| `service` equals the producer | deploy-time error naming `tail_consumers service must be another worker` |
| Snapshot target missing, self or duplicate | `DESIRED_TAIL_CONSUMER_SCOPE` / `DESIRED_DUPLICATE` |
| Render-time dangling target | `CONFIG_UNRESOLVED_SERVICE` |

## Examples and tests

Producer:

```js
export default {
  async fetch() {
    console.log("hello from producer");
    return new Response("ok");
  },
};
```

Consumer:

```js
export default {
  async tail(events, env, ctx) {
    ctx.waitUntil(
      env.LOGS.put(
        `batch:${Date.now()}`,
        JSON.stringify(events),
      ),
    );
  },
};
```

Coverage:

## Where it runs

- `vrt-local` resolves each `{ service = "<name>" }` against the pack's
  worker table (the same `resolve_worker` path `services` bindings use) and
  stores the resulting worker ids on the **version record** — the consumer
  set is versioned state, so rollback restores the consumers that version
  had, matching Cloudflare's "consumer list is a property of the version"
  semantics.
- `vrt-config` emits every `tail_consumers` entry as a
  `tails = [(name = "<service>")]` designator on the producer's workerd
  service — merged into the same list as the platform's internal log
  tail, so user consumers and log collection coexist — and every
  `streaming_tail_consumers` entry as a `streamingTails` designator
  (a separate list; see the upstream caveat below).
- workerd then delivers `TailEvent` batches to `tail` consumers and
  TailStream events to `tailStream` consumers itself; the platform only
  routes and stores.
- Deployments carrying consumers require the `tailConsumers` /
  `streamingTailConsumers` cell capability respectively; a cell that
  does not claim it rejects the assignment at admission rather than
  half-rendering.

## Backends

There is no platform-side queue: events flow producer isolate → consumer
isolate inside the tenant's workerd process. The platform's internal
per-worker log tail (`vrt-tail-<worker>`) is just another entry in the same
`tails` list and is unaffected by user consumers. The platform collector
honours `[observability]`: when the worker's `enabled` or
`logs.enabled` is `false` no collector is rendered at all (nothing is
collected); otherwise an `OBS` text binding carries
`{"headSamplingRate":<float>, "invocationLogs":<bool>}` and the embedded
collector samples per invocation — a dropped invocation keeps none of its
log/exception/outcome records (deterministic FNV-1a over the TraceItem's
`scriptName` + `eventTimestamp` + `durableObjectId`; the pinned workerd
exposes no `traceId`/`spanId` on `tail()` events, so the verdict is stable
for a re-delivered event), and `invocation_logs=false` drops only the
outcome record. The wire carries the rate as integer ppm
(`headSamplingRatePpm`, 0..1_000_000) because the signed snapshot's
canonical JSON accepts safe integers only.

## Known differences from Cloudflare

- Consumers must live in the **same tenant**; Cloudflare's cross-account
  tails (shared `tail_consumers` across accounts) are rejected as
  unresolvable names.
- Delivery is best-effort like Cloudflare: workerd may drop events under
  backpressure and consumer failures are isolated from the producer —
  both semantics come from the same upstream code path.
- A self-referencing consumer is **rejected** at pack time and in the
  signed-desired validator: a worker whose `tails`/`streamingTails`
  designator points at itself would re-enter its own handler for every
  event with no documented recursion bound. Point the consumer at a
  sibling worker instead.
- The same consumer worker may not appear on **both** channels
  (`tail_consumers` and `streaming_tail_consumers`): workerd would
  invoke its `tail()` and `tailStream()` for one event, double-delivering
  with no way to tell the copies apart. Pick one channel per consumer;
  the pack and the desired validator both fail closed.
- Duplicate entries inside one list are collapsed by name at
  normalization (`vrt-local` pack); a hand-crafted signed snapshot that
  still carries duplicates is rejected by the desired validator. Both
  lists are capped at 16 resolved consumers per worker.

## Unsupported keys and error shape

- Each `tail_consumers[]` / `streaming_tail_consumers[]` entry accepts
  only `service` — anything else → `VRT_CONFIG_UNSUPPORTED_FIELD`
  (`tail_consumers[0].<key>` / `streaming_tail_consumers[0].<key>`).
- A missing/non-string `service` → `VRT_CONFIG_INVALID`
  (`tail_consumers[0].service`); a `service` name that resolves to no
  worker in the tenant fails the pack (`tail consumer target` /
  `streaming tail consumer target`), and at desired-state validation a
  consumer id that is unknown or belongs to a different tenant →
  `DESIRED_TAIL_TARGET_SCOPE`.
- A cell not claiming the `tailConsumers` / `streamingTailConsumers`
  capability is never assigned a worker that needs it — the deploy is
  rejected at admission, and a rollback whose target version still needs
  it re-validates the same bit.
- In dry-run rendering a worker with an unresolvable consumer is skipped
  individually (poisoned-worker isolation) while siblings still render.

## Upstream caveat (`streamingTails`)

The pinned `workerd.capnp` annotates the field we emit with
"NOTE: This will be deleted in a future refactor, do not depend on
this." We expose `streaming_tail_consumers` anyway because the channel
is wired and serving TailStream events on the pinned fork — but treat
it as provisional:

- **Tripwire**: a workerd pin bump that deletes `streamingTails` (or
  renames it) breaks the render — the emitted capnp fails workerd's
  own schema check, so the pin-bump checklist must grep the new
  `workerd.capnp` for the field before shipping. `vrt-config`'s
  designator emit (`capnp.rs` `streamingTails = [...]`) is the single
  point to update or gate.
- **Removal follow-up**: if upstream removes the channel, the desired
  field stays schema-compatible (skip-serialized when empty) while the
  render stops emitting designators; a deprecation note must land here
  before any behavior change.
- **`tailStream` limits**: events stream per invocation — no batching
  like `tail()`'s `TailEvent[]`, and `TailItem`-level metadata differs
  from the `tail()` shape (check the pinned schema for the current
  event fields rather than assuming parity).

## Examples and tests

- `vrt-config/tests/tail_emit.rs` — `tails`/`streamingTails` designator
  emit, merge with the platform log tail, unresolvable-consumer worker
  isolation.
- `vrt-local/src/project_cfg.rs` — `tail_consumers` /
  `streaming_tail_consumers` parse vectors (unknown-key fail-closed,
  non-string `service`).
- `snapshot-protocol/src/compute_desired.rs` — same-tenant existence,
  self-reference, duplicate (within one list and across both), cap and
  empty-list vectors.
- `vrt-local/src/worker_pkg.rs` — shared `resolve_tail_consumers`
  invariants (self-target, dedupe, 16 cap, dual-channel refusal).
- `vrt-local/src/state.rs` — version identity covers both lists.
- `vrt-host/tests/streaming_tail_workerd.rs` — real-workerd
  `tailStream()` delivery (gated on `VRT_WORKERD_BIN`).
- `shared/vrt-project-config-vectors.json` — cross-repo golden vectors
  (parsed identically by the Rust and TypeScript project configs).

## Changelog

- 2026-11-21: version identity is derived from the canonical serialized
  `VersionRecord`, so both tail lists (and any future version-scoped
  field) automatically mint a new version on change; both lists share
  one validator (self-reference refused, dedupe, 16 cap); a consumer on
  both channels is refused instead of double-delivered; sampling docs
  corrected to the real TraceItem fields; real-workerd `tailStream`
  smoke added.
