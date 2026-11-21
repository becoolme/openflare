# Queues

Queue producers and consumers backed by `vrt-sql`, delivered to
`queue(batch, env, ctx)` handlers through a lease-gated dispatcher.

**Status: Partial**

## Wrangler configuration

From `examples/standalone-queue/wrangler.jsonc`:

```jsonc
{
  "queues": {
    "producers": [{ "binding": "Q", "queue": "e2e-queue" }],
    "consumers": [
      { "queue": "e2e-queue", "max_batch_size": 5, "max_batch_timeout": 1 }
    ]
  }
}
```

## Where it runs

- Producer: `env.Q.send(...)` / `sendBatch(...)` go through the native
  workerd `queue` binding into `facade-queue`, then `vrt-provider`'s
  `/internal/bindings/v1/queue/{bindingId}/{message|batch|metrics}`
  route and `vrt-sql`'s `QueueBackend` — one SQLite database per queue.
- Consumer: the dispatcher (`vrt serve` standalone, `cell-controller`
  on a cell) claims batches from `vrt-sql` inside the same transaction
  as its dispatch lease, posts to `vrt-host` `/v1/queue/dispatch`, which
  reaches `facade-qconsumer` over `sock-qconsumer-<workerId>` and calls
  the worker's `queue(batch)` handler via `Fetcher.queue()`.
- Settlement mirrors the miniflare broker semantics: non-`ok` outcomes
  or `retryBatch` retry the whole batch; `explicitRetries` and
  `retryMessages` retry individual messages (message delay > batch delay
  > consumer `retry_delay`); messages that exhaust `max_retries + 1`
  deliveries move to the dead-letter queue or drop with an audit record.

## Backends

`vrt-sql` queue storage only. The dispatcher is `cell-controller`'s
lease-gated dispatcher on a cell (at most one per queue per cell) or the
standalone `vrt serve` loop (1 s cadence).

## Known differences from Cloudflare

- `queue` and `dead_letter_queue` are **display names**. Real queue ids
  are derived at pack time (`sha256("vrt-q:{tenant}:{name}")[:26]`) and
  covered by the signed snapshot — a client can never name an arbitrary
  queue id. Producers in the same instance that name the same queue
  share it, matching Cloudflare semantics.
- Delivery is at-least-once by construction: a crash between delivery
  and settlement re-delivers after the message lease (~90 s default).
- Backend settlement errors keep the source message leased and apply a
  bounded exponential pause (up to 64 s) before the lane claims more work.
  Only source-queue `ack`/`retry`/`dead-letter` failures pause the lane: a
  failed dead-letter *forward* (the DLQ is a different queue) leaves the
  exhausted message leased for redelivery and does not stall unrelated
  claims.
- Per-batch handler budget is ~55 s inside `facade-qconsumer`
  (`facade/facade-qconsumer.ts`); the dispatch claim TTL sits above it.
  Cloudflare's exact consumer wall-clock limits are not replicated.
- Wire/message bounds mirror the miniflare broker: ≤ 128,000 B per
  message, ≤ 100 messages and ≤ 288,000 B per batch, delay 0–86,400 s,
  content types `text | json | bytes | v8` (`vrt-sql/src/queue.rs`).
- Consumer field bounds (validated both at normalize and in the signed
  snapshot): `max_batch_size` 1–100, `max_batch_timeout` 0–60 s,
  `max_retries` 0–100, `retry_delay` 0–86,400 s, `max_concurrency`
  1–1000. `dead_letter_queue` must differ from `queue`.
- `queues.consumers[].max_concurrency` is honored by the dispatcher; the
  standalone dispatcher runs on a fixed 1 s tick inside `vrt serve`.

## Unsupported keys and error shape

- `queues` may only contain `producers` and `consumers` — anything else
  → `VRT_CONFIG_UNSUPPORTED_FIELD`.
- Producer entries accept only `binding` and `queue` (so
  `delivery_delay` etc. → `VRT_CONFIG_UNSUPPORTED_FIELD`).
- Consumer entries accept only `queue`, `max_batch_size`,
  `max_batch_timeout`, `max_retries`, `dead_letter_queue`,
  `retry_delay`, `max_concurrency` — anything else (e.g. `max_ack`) →
  `VRT_CONFIG_UNSUPPORTED_FIELD`.

## Examples and tests

- `examples/standalone-queue` — producer + consumer in one worker with
  `max_batch_size` / `max_batch_timeout`.
- `facade/test/queue.test.mjs` — facade queue contract tests.
- `vrt-provider/tests/queue_wire.rs`, `vrt-provider/tests/queue_e2e.rs`
  — provider wire and end-to-end tests.
- `vrt-sql/tests/queue_tests.rs` — queue backend tests.
- `vrt-host/tests/delivery_e2e.rs` — dispatch/delivery end-to-end tests.
