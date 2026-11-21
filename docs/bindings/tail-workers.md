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

- `vrt-local/tests/tail_consumers.rs` — wrangler object/string forms,
  dedupe, empty list, unknown/self/malformed/over-cap rejection.
- `vrt-config/tests/tail_consumers_emit.rs` — designator rendering,
  internal-collector coexistence and fail-closed validation.
- `vrt-config/tests/golden/tenant-tails.capnp` — byte-for-byte golden
  with two consumers plus the platform collector.
- `vrt-host/tests/tail_consumers.rs` — assemble assertions always; with
  `VRT_WORKERD_BIN` set, real workerd proves the consumer receives a
  `TailEvent` carrying the producer's `console.log`.
