# Cron Triggers

`triggers.crons` schedules delivered to the worker's `scheduled()`
handler by the cell's cron runner.

**Status: Drop-in**

## Wrangler configuration

From `examples/standalone-cron/wrangler.jsonc`:

```jsonc
{
  "triggers": { "crons": ["* * * * *"] }
}
```

Native `vrt.toml` equivalent:

```toml
[[crons]]
scheduleId = "cron:edge-api:0"
expression = "*/15 * * * *"
```

## Where it runs

- `cell-controller`'s `CronRunner` (platform) or the `vrt serve` cron
  loop (standalone, 30 s tick) walks the signed desired snapshot once
  per minute and fires each matching schedule.
- Delivery: `POST /v1/cron/trigger` on the host's loopback control
  listener → `sock-cron-<workerId>` → `facade-cron-<workerId>` →
  `env.TARGET.scheduled()` — the only workerd mechanism that produces a
  real scheduled event (`vrt-host/src/cron_trigger.rs`). Ingress HTTP
  posts to `__vrt/scheduled` or `/cdn-cgi/handler/scheduled` hit
  `fetch`, never `scheduled`, so the channel cannot be forged from
  outside.
- The runner claims each `(workerId, versionId, scheduleId,
  scheduledTime)` in a durable log (`FileTriggerLog`) — at-most-once per
  minute slot — and the host dedupes again on the
  `x-vrt-cron-idempotency-key` header, so a lost response cannot
  double-fire.
- A cron trigger wakes a hibernated tenant (`WakeSourceKind::Cron`).

Runtime requirement: `facade-cron-<workerId>` carries the
`$experimental` `service_binding_extra_handlers` flag (it enables
`Fetcher.scheduled()`), so workerd only loads the rendered config with
`--experimental` on argv. The derivation lives in
`vrt_config::launch_args` and is shared by the serve path
(`vrt-host::assemble`) and the deploy-time config check
(`vrt-local` preflight) — a cron-only tenant must never need an
operator-supplied flag.

## Backends

None — no data plane. The claim log lives under the state root
(`state/cron`); the schedule source of truth is the signed
`compute.desired` snapshot.

## Known differences from Cloudflare

- Expressions use Cloudflare's 5-field cron format (`min hour dom mon dow`,
  names `JAN`-`DEC` / `SUN`-`SAT`, `*`, lists, ranges, and steps), at minute
  granularity — validated at deploy in `snapshot-protocol`'s
  `compute_desired`. Numeric weekdays follow Cloudflare's mapping: `1` is
  Sunday through `7` Saturday; `0` is invalid. Cloudflare's day modifiers
  `L`, `LW`, `nW`, `dL`, and `d#n` are supported. There is no seconds field.
  Other syntax such as `?` or a year field is rejected during validation.
- Standalone derives `scheduleId` deterministically (`cfg:{name}:{i}`)
  from the wrangler `triggers.crons` array index.
- `scheduledTime` is the canonical minute the cron fires on; the worker
  sees it and the expression in the event payload.
- Upstream timeout for the scheduled call tracks ~Cloudflare's cron
  wall-clock bound (`vrt-host/src/cron_trigger.rs`).

## Unsupported keys and error shape

- `triggers` may only contain `crons` — anything else (e.g.
  `triggers.queue`) → `VRT_CONFIG_UNSUPPORTED_FIELD`.
- `crons` must be an array of non-empty strings → `VRT_CONFIG_INVALID`.
- An invalid expression fails desired-state validation
  (`invalid cron expression`).

## Examples and tests

- Supported modifiers: `0 0 L * *` (last day), `0 0 LW * *` (last
  weekday), `0 0 15W * *` (weekday nearest the 15th), `0 18 * * 6L`
  (last Friday), and `0 0 * * MON#2` (second Monday).
- `examples/standalone-cron` — `* * * * *` trigger writing to D1 as a
  durable side effect.
- `vrt-host/tests/cron_smoke.rs` — cron end-to-end smoke test (skips
  without a real workerd).
- `cell-controller/src/cron.rs` — runner, matcher, claim log and
  transport (unit-tested expression matcher).

## Changelog

- 2026-11-20: Align numeric weekday matching and validation with Cloudflare (`1` = Sunday, `7` = Saturday).
- 2026-11-20: Add Cloudflare cron day modifiers `L`, `LW`, `nW`, `dL`, and `d#n` to validation and scheduling.
