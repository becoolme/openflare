# Unverified behaviors

Behaviors that are **plausibly Cloudflare-compatible but lack a
sufficient citation** (official docs or a workerd repo test) — the
rule for `cases/` is that every `expect` must cite its basis in the
case's `evidence` list. Each entry below names the gap; when the
reference is found, promote it to a case.

| # | Area | Behavior in question | Why it is not asserted yet |
| --- | --- | --- | --- |
| 1 | KV | Exact error text when `get(key,"json")` hits non-JSON content (CF throws, message shape uncited) | Only generic TypeError codes are cited (`facade/kv/transport.ts`) |
| 2 | KV | `cacheTtl` semantics — edge/disk caching hint <30 s rejected (`KV_INVALID_OPTIONS` is asserted; the actual caching effect is not) | No public doc fixes observable hit/miss timing |
| 3 | R2 | `uploadedBefore`/`uploadedAfter`/`secondsGranularity`/`httpHeaders` condition branches | covered partially by `r2_s3_conditional.rs` for puts; `get` side uncited |
| 4 | Cache | `max-age=1` real expiry: TTL eviction vs. lazy miss-on-read | cache.rs honors s-maxage/max-age but a wall-clock expiry assertion would be timing-flaky |
| 5 | Cron | `event.retry`/`noRetry` on scheduled() controller | not surfaced by the standalone CronRunner path yet |
| 6 | DO | `storage.sql` KV↔SQL visibility interplay (whether a KV write and a SQL row are visible across the two surfaces of the same object) | `do-storage` exercises `sql.exec` and KV separately; no CF doc fixes cross-surface visibility |
| 7 | DO | `blockConcurrencyWhile` reject → surface to waiting fetch (error propagation shape) | needs a failing init fixture; added when a workerd-side citation is found |
| 8 | Cron | `ScheduledEvent.type` (`"scheduled"`) on the service-worker syntax; the module `ScheduledController` has no `type` field, so `cron-basic` does not assert it | module syntax only in this suite |
| 9 | Service bindings | Calling a second Worker through default and named service entrypoints, including cross-Worker failure behavior | the standalone harness deploys one project at a time and cannot provision a target Worker as a dependency |
| 10 | Hyperdrive | PostgreSQL connection behavior, pooling, and credential rotation through a Hyperdrive binding | requires a reachable PostgreSQL service and rotation lifecycle that the conformance stack does not provision |
| 11 | Rate limiting | Counter sharing across isolates/instances and the exact reset window under concurrent requests | `ratelimit-basic` covers only the documented `limit({key})` result shape; the local adapter is in-process and cannot establish Cloudflare's distributed consistency |
| 12 | Secrets | Secret injection, rotation, and redaction from logs or errors | the harness has no isolated secret provisioning/rotation path; plain `vars` coverage does not establish secret handling |
| 13 | WebSockets | Upgrade acceptance, bidirectional message lifecycle, close codes, and Durable Object hibernation | the HTTP-only runner cannot drive a WebSocket client or assert hibernation across process lifecycle |
| 14 | Workflows | Run creation, durable step persistence, retry, and terminal state behavior | workflow orchestration is not connected to the standalone runner or a persistent workflow store |
| 15 | Assets | `run_worker_first` fallthrough, exclusion patterns, and advanced routing behavior | `assets-basic` covers only a successful `env.ASSETS.fetch()` for one packaged file |
| 16 | Browser | CDP surface beyond launch/connect + screenshot: `Runtime.evaluate`, `printToPDF`, multi-target navigation; engine errors mid-session (`engine_error`/`pipe_error` close reasons) and abnormal socket drop without a close frame | `browser-basic` pins the full `/v1/*` registry surface incl. 404/409/410/429 edges, concurrent sessions, and the `client_closed` history path; kitesurf's CDP coverage is upstream-incomplete so deeper assertions would test engine parity, not the binding layer |
