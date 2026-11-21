# Tail Workers

`tail_consumers` attaches a worker's trace events to sibling workers in the
same tenant — after the producer's invocation finishes, workerd replays a
summary of what happened (outcome, exceptions, logs, event metadata) to each
consumer's `tail` handler. `streaming_tail_consumers` does the same for
consumers exporting a `tailStream` handler, which receives a live event
stream instead of post-invocation batches. Consumers observe; they never
affect the producer's response.

**Status: Partial** — same-tenant consumer resolution, `tail`/`tailStream`
entrypoint delivery and rollback-safe versioning work; cross-account
consumers are not supported.

## Wrangler configuration

```toml
tail_consumers = [
  { service = "log-sink" },
  { service = "log-sink-2" },
]

streaming_tail_consumers = [
  { service = "live-sink" },
]
```

`service` is the **name of another worker in the same tenant** (or the
producer itself). It is resolved to a concrete worker at pack/deploy time —
the wire carries a worker id, never a raw name, so a renamed or deleted
target can never silently receive events.

The `tail_consumers` consumer exports a `tail` handler; a
`streaming_tail_consumers` consumer exports `tailStream`:

```js
export default {
  async tail(events, env, ctx) {
    for (const ev of events) {
      console.log(ev.outcome, ev.exceptions?.length ?? 0);
    }
  },
};
```

```js
export default {
  tailStream(event, env, ctx) {
    // `event` is the onset event for this invocation; return a handler
    // (function or object) that receives the session's remaining
    // TailStream events as they happen.
    return {};
  },
};
```

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
  (a separate list; upstream marks the channel for future rework but it
  is wired and serving TailStream events today).
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
log/exception/outcome records (deterministic FNV-1a over `traceId`/`spanId`
when present, `Math.random()` otherwise), and `invocation_logs=false` drops
only the outcome record. The wire carries the rate as integer ppm
(`headSamplingRatePpm`, 0..1_000_000) because the signed snapshot's
canonical JSON accepts safe integers only.

## Known differences from Cloudflare

- Consumers must live in the **same tenant**; Cloudflare's cross-account
  tails (shared `tail_consumers` across accounts) are rejected as
  unresolvable names.
- Delivery is best-effort like Cloudflare: workerd may drop events under
  backpressure and consumer failures are isolated from the producer —
  both semantics come from the same upstream code path.
- A self-referencing consumer is allowed even on first deploy, because the
  deploy itself produces the version that satisfies the reference
  (matches `wrangler deploy` behavior).

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

## Examples and tests

- `vrt-config/tests/tail_emit.rs` — `tails`/`streamingTails` designator
  emit, merge with the platform log tail, unresolvable-consumer worker
  isolation.
- `vrt-local/src/project_cfg.rs` — `tail_consumers` /
  `streaming_tail_consumers` parse vectors (unknown-key fail-closed,
  non-string `service`).
- `snapshot-protocol/src/compute_desired.rs` — same-tenant existence,
  duplicate-consumer and empty-list vectors.
- `shared/vrt-project-config-vectors.json` — cross-repo golden vectors
  (parsed identically by the Rust and TypeScript project configs).
