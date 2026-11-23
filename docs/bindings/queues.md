# Queues

Queue producers and consumers backed by `vrt-sql`, delivered to
`queue(batch, env, ctx)` handlers through a lease-gated dispatcher.

**Status: Partial**

## Wrangler configuration

From `examples/standalone-queue/wrangler.jsonc`:

```jsonc
{
  "queues": {
    "producers": [
      { "binding": "Q", "queue": "e2e-queue", "delivery_delay": 30 }
    ],
    "consumers": [
      { "queue": "e2e-queue", "max_batch_size": 5, "max_batch_timeout": 1 }
    ]
  }
}
```

The native standalone shape is:

```toml
[bindings.queue]
# Legacy plain form: no queue-level default delay.
LEGACY = "e2e-queue"
# Table form with a queue-level default delay, 0..=86400 seconds.
Q = { queue = "e2e-queue", deliveryDelaySeconds = 30 }
```

`delivery_delay` (wrangler) / `deliveryDelaySeconds` (native `vrt.toml`) is a
queue-level default send delay: a `send()`/`sendBatch()` call that does not
pass its own `delaySeconds` inherits it. The queue-level value only applies
when the caller omits a delay — workerd only emits `X-Msg-Delay-Secs` when
`delaySeconds` is present, so `facade-queue` fills the header with the
queue-level default only when it is absent. An explicit per-message (or
per-batch) `delaySeconds` always wins, including an explicit `0` (which
means "no delay", Cloudflare's documented floor). Values outside `0..=86400`
are rejected at config load.

## Where it runs

- Producer: `env.Q.send(...)` / `sendBatch(...)` go through the native
  workerd `queue` binding into `facade-queue`, then `vrt-provider`'s
  `/internal/bindings/v1/queue/{bindingId}/{message|batch|metrics}`
  route and `vrt-sql`'s `QueueBackend` — one SQLite database per queue.
- Consumer: the dispatcher (`of serve` standalone, `cell-controller`
  on a cell) claims batches from `vrt-sql` inside the same transaction
  as its dispatch lease, posts to `vrt-host` `/v1/queue/dispatch`, which
  reaches `facade-qconsumer` over `sock-qconsumer-<workerId>` and calls
  the worker's `queue(batch)` handler via `Fetcher.queue()`.
- Settlement mirrors Cloudflare's per-message precedence: an explicit
  `ack()` remains acknowledged when a later message fails or the handler
  returns an error. `ackAll()` has the same precedence at batch scope --
  the implicit retry of a failed outcome does not resurrect an
  acknowledged batch, while `retry()`/`retryMessages` marks recorded
  before it still retry. Otherwise non-`ok` outcomes or `retryBatch`
  retry the whole batch; `explicitRetries` and
  `retryMessages` retry individual messages (message delay > batch delay
  > consumer `retry_delay`); messages that exhaust `max_retries + 1`
  deliveries move to the dead-letter queue or drop with an audit record.

## Backends

`vrt-sql` queue storage only. The dispatcher is `cell-controller`'s
lease-gated dispatcher on a cell (at most one per queue per cell) or the
standalone `of serve` loop (1 s cadence).

## Known differences from Cloudflare

- `queue` and `dead_letter_queue` are **display names**. Real queue ids
  are derived at pack time (`sha256("vrt-q:{tenant}:{name}")[:26]`) and
  covered by the signed snapshot — a client can never name an arbitrary
  queue id. Producers in the same instance that name the same queue
  share it, matching Cloudflare semantics.
- Delivery is at-least-once by construction: a crash between delivery
  and settlement re-delivers after the message lease (~90 s default).
- Messages are retained for at most four days from `created_at`, matching
  Cloudflare's maximum retention. Dead-letter rows use four days from
  `dead_at`. Expired rows are purged lazily by `send`, `claim` and
  `metrics` in the same transaction, so queue visibility never counts an
  expired message; there is no independent queue-storage sweeper.
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
  `delaySeconds` (and the `X-Msg-Delay-Secs` / per-item `delaySecs`
  wire field) follows the documented range: a negative or >86,400 value
  fails the send with a queue error, `0` is the floor and means "no
  delay", and a delayed message's `Message.timestamp` stays the send
  time, so the delivery lag is the applied delay
  (`conformance/cases/queues/queues-basic`).
- Queue-level default delay: `delivery_delay` (wrangler) /
  `deliveryDelaySeconds` (native `vrt.toml`) rides in the signed queue
  binding and `facade-queue` applies it only when workerd sent no
  `X-Msg-Delay-Secs` header. Explicit `send`/`sendBatch` delays always
  win, including an explicit `0`.
- `sendBatch` is all-or-nothing. The provider validates every item
  (message count, per-message and batch bytes, content type, delay,
  client-supplied id/timestamp) before it renders the single vrt-sql
  send request, and `vrt-sql` validates the whole batch before opening
  the transaction. One invalid item rejects the call and enqueues
  nothing, matching the single `sendBatch` promise (a resolved call has
  written the whole batch). See `conformance/cases/queues/queues-batch`.
- `queues.consumers[].max_concurrency` caps in-flight consumer batches
  per queue lane: the dispatcher stops claiming and dispatching while
  `inflight` equals the configured value, so `max_concurrency: 1`
  serializes handler invocations (`cell-controller/src/queue_dispatch.rs`,
  `conformance/cases/queues/queues-batch`).
- Consumer field bounds (validated both at normalize and in the signed
  snapshot): `max_batch_size` 1–100, `max_batch_timeout` 0–60 s,
  `max_retries` 0–100, `retry_delay` 0–86,400 s, `max_concurrency`
  1–1000. `dead_letter_queue` must differ from `queue`.
- An unset `max_concurrency` defaults to 10 in-flight batches on this
  platform (Cloudflare documents "scale to the currently supported
  maximum" instead); the standalone dispatcher runs on a fixed 1 s tick
  inside `of serve`.

## Unsupported keys and error shape

- `queues` may only contain `producers` and `consumers` — anything else
  → `VRT_CONFIG_UNSUPPORTED_FIELD`.
- Producer entries accept `binding`, `queue`, and `delivery_delay`
  (0–86,400 seconds; anything else, e.g. `max_ack`, →
  `VRT_CONFIG_UNSUPPORTED_FIELD`).
- Consumer entries accept only `queue`, `max_batch_size`,
  `max_batch_timeout`, `max_retries`, `dead_letter_queue`,
  `retry_delay`, `max_concurrency` — anything else (e.g. `max_ack`) →
  `VRT_CONFIG_UNSUPPORTED_FIELD`.

## Examples and tests

- `examples/standalone-queue` — producer + consumer in one worker with
  `max_batch_size` / `max_batch_timeout`.
- `conformance/cases/queues/queues-basic` — send/sendBatch, delay bounds.
- `conformance/cases/queues/queues-batch` — sendBatch atomicity and
  `max_concurrency` serialization.
- `facade/test/queue.test.mjs` — facade queue contract tests.
- `vrt-provider/tests/queue_wire.rs`, `vrt-provider/tests/queue_e2e.rs`
  — provider wire and end-to-end tests.
- `vrt-sql/tests/queue_tests.rs` — queue backend tests.
- `vrt-host/tests/delivery_e2e.rs` — dispatch/delivery end-to-end tests.
