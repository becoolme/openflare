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
| 3 | R2 | `ssecKey` (SSE-C) end-to-end; the wire carries it but no CF Workers doc commits to behavior | vendor surface only |
| 4 | R2 | `uploadedBefore`/`uploadedAfter`/`secondsGranularity`/`httpHeaders` condition branches | covered partially by `r2_s3_conditional.rs` for puts; `get` side uncited |
| 5 | D1 | `dump()` / `raw({columnNames:false})` variants and `D1_RESULT_TOO_LARGE` limits | workerd-d1 surface exists; limit values differ by deployment |
| 6 | Cache | `max-age=1` real expiry: TTL eviction vs. lazy miss-on-read | cache.rs honors s-maxage/max-age but a wall-clock expiry assertion would be timing-flaky |
| 7 | Cron | `event.retry`/`noRetry` on scheduled() controller | not surfaced by the standalone CronRunner path yet |
| 8 | DO | `storage.sql` KV↔SQL visibility interplay and alarm persistence across a process restart | restart persistence is proven manually; an automated restart step is not wired into the runner yet |
| 9 | DO | `blockConcurrencyWhile` reject → surface to waiting fetch (error propagation shape) | needs a failing init fixture; added when a workerd-side citation is found |
| 10 | R2 | Whether `R2MultipartUpload.complete()` resolves with `httpMetadata`/`customMetadata` populated (the native adapter strips them; `r2-multipart` reads metadata back via `get()` instead) | no CF doc fixes the complete() result fields |
| 11 | Cron | `ScheduledEvent.type` (`"scheduled"`) on the service-worker syntax; the module `ScheduledController` has no `type` field, so `cron-basic` does not assert it | module syntax only in this suite |
| 12 | Service bindings | Calling a second Worker through default and named service entrypoints, including cross-Worker failure behavior | the standalone harness deploys one project at a time and cannot provision a target Worker as a dependency |
| 13 | Hyperdrive | PostgreSQL connection behavior, pooling, and credential rotation through a Hyperdrive binding | requires a reachable PostgreSQL service and rotation lifecycle that the conformance stack does not provision |
| 14 | Rate limiting | Counter sharing across isolates/instances and the exact reset window under concurrent requests | `ratelimit-basic` covers only the documented `limit({key})` result shape; the local adapter is in-process and cannot establish Cloudflare's distributed consistency |
| 15 | Secrets | Secret injection, rotation, and redaction from logs or errors | the harness has no isolated secret provisioning/rotation path; plain `vars` coverage does not establish secret handling |
| 16 | WebSockets | Upgrade acceptance, bidirectional message lifecycle, close codes, and Durable Object hibernation | the HTTP-only runner cannot drive a WebSocket client or assert hibernation across process lifecycle |
| 17 | Workflows | Run creation, durable step persistence, retry, and terminal state behavior | workflow orchestration is not connected to the standalone runner or a persistent workflow store |
| 18 | Assets | `run_worker_first` fallthrough, exclusion patterns, and advanced routing behavior | `assets-basic` covers only a successful `env.ASSETS.fetch()` for one packaged file |
