# Vendored: Cloudflare Workflows engine (miniflare build)

These artifacts are the compiled single-file bundles miniflare ships
for its local Workflows support. The engine runs as a Durable Object
inside the tenant workerd, and the wrapped binding presents the
user-facing `env.<workflowBinding>` API. `binding.worker.js` stays as
the upstream+W1/W2 reference; the emitted engine is the documented
`binding.vrt.worker.js` fork below.

W1-r2 added `zod.worker.js` (miniflare `dist/src/workers/shared/`):
`binding.worker.js` imports `miniflare:zod` at module scope (its zod
schemas validate workflow event payloads); without it every engine
service failed config load and took the whole tenant down. It is
registered as an `internal = true` module in `config.extensions` —
the same slot miniflare uses (`index.js` `#assembleConfig`).

- `zod.worker.js` bundles zod `4.4.3` (MIT, `LICENSE.zod`) plus the
  miniflare `zod.worker.ts` shim. Its only import,
  `node-internal:internal_buffer`, resolves inside workerd's internal
  module registry — extension modules are privileged and do NOT need
  `nodejs_compat`; miniflare likewise gives the engine only
  `experimental` + `compatibilityDate 2024-10-22`.

## Source

| Field | Value |
| ----- | ----- |
| Upstream project | `cloudflare/workers-sdk` — `packages/workflows-shared` (MIT) |
| Distribution     | `miniflare@5.20260916.0-alpha` (`dist/src/workers/workflows/*.worker.js`) |
| Paired workerd   | `workerd@1.20260916.1` (miniflare package metadata workerd pin) |
| VRT pinned workerd | `workerd@1.20261103.1` (`deploy/standalone/workerd`, B8) — the vendored bundle is older than the runtime it runs on; the API surface it uses (`ctx.storage.sql`, alarms, `WorkflowEntrypoint`, service bindings) is stable across the pinned versions. Verified by `vrt-host/tests/workerd_entry_smoke.rs::real_workerd_workflow`. |

- `binding.worker.js` exports `Engine` (the DO class) and
  `WorkflowBinding` (the service entrypoint the wrapped binding calls);
  it remains the byte-pinned upstream+W1/W2 reference.
- `binding.vrt.worker.js` is the VRT engine fork (same exports) emitted
  as `workflows.mjs`; it adds terminal-instance retention cleanup.
- `wrapped-binding.worker.js` exports `makeBinding` (the
  `WrappedBinding` factory; produces the `env.WF`-shaped API).
- `zod.worker.js` exports `z` (the zod namespace the engine imports
  as `miniflare:zod`). `miniflare:shared` (`index.worker.js`) is NOT
  vendored — nothing in the import closure references it
  (`workflows_vendor::import_closure` enforces this structurally).

## Integrity

`SHA256SUMS` pins the exact bytes of all four artifacts; `vrt-config`
tests re-hash the files and fail closed on any drift. Never edit a
vendored file in place — upgrade it as a unit (see below). The
sanctioned local changes are the W1-r5 / W2 P0-0 patches in
`binding.worker.js` and the VRT retention fork in
`binding.vrt.worker.js`.

## VRT fork: terminal retention + concurrency (binding.vrt.worker.js)

Cloudflare retains completed/errored instance state for a bounded period.
This engine has no such cleanup. The fork adds a per-instance retention
deadline without changing the upstream reference file:

- `Engine.setStatus` stores `VRT_RETENTION_AT` on the transition to
  `Complete` / `Terminated` / `Errored` and arms the actor alarm; the
  delay is `WORKFLOW_SUCCESS_RETENTION_MS` /
  `WORKFLOW_ERROR_RETENTION_MS`, defaulting to seven days.
- `Engine.alarm()` deletes all instance storage once that deadline is due;
  otherwise it runs the existing priority-queue resume and re-arms the
  retention deadline.
- `Queued` / `Running` clears the marker, so a restarted instance cannot
  inherit a stale cleanup deadline.

The fork also exports a `Coordinator` Durable Object per Workflow. It keeps
an SQLite table of active instance ids and caps them at
`WORKFLOW_CONCURRENCY_LIMIT` (default 4,500). `create()` fails with
`workflow.concurrency_limit` when full; terminal status, explicit delete, or
retention cleanup releases the slot idempotently.

The signed workflow declaration can set `successRetentionMs` /
`errorRetentionMs` (max 30 days) and `concurrencyLimit`; per-instance
retention is not wired yet. Absent values use seven days and 4,500.

## Local patch: W1-r5 durable resume (binding.worker.js only)

Upstream's `TimePriorityQueue.handleNextAlarm()` is a peek-only stub,
and no code path re-invokes `Engine.init` once the process restarts:
`status()` is a pure read, the in-memory `scheduler.wait` for
`step.sleep`/retry/event-wait deadlines dies with the process, and
workerd never loads the actor again on its own. Result: any restart
whose deadline passed while the process was down left the instance
`"status":"running"` forever (standalone e2e symptom, reproduced in
`vrt-host/tests/do_w1r4_workerd.rs::workflow_resumes_when_restart_lands_past_sleep_deadline`).

The patch completes the semantics the stub was written for, without
new dependencies or a supervisor-side hook:

- `handleNextAlarm` now arms the actor's durable alarm to the heap
  head (`ctx.storage.setAlarm(head.targetTimestamp)`), or
  `deleteAlarm()` when empty. Re-armed after every `add`.
- `Engine.alarm()` delivers the wake through a single-flight
  `#resumeFromStorageIfNeeded()` — `init` is re-driven from persisted
  `INSTANCE_METADATA`, with `init`'s own `isRunning`/terminal-status
  gates making it idempotent and never resurrecting
  errored/terminated/complete/paused instances. `alarm()` also
  pops past entries and re-arms to the current head — a
  perpetually-past head would otherwise make `setAlarm` re-fire
  instantly in a busy loop (bounded: the heap drains to empty).
- The `Engine` constructor schedules the same single-flight resume on
  every actor load (`ctx.waitUntil`), so a `status()` poll or any RPC
  touch also resumes — covering wakes with no armed alarm.

worker semantics relied on: a persisted `setAlarm` fires on workerd
startup even when its deadline already passed, with zero traffic
(verified against `workerd@1.20260916.1` by a minimal DO alarm
experiment). Replaying an overdue sleep waits
`entryPQ.targetTimestamp - Date.now()` (~0) and completes — deadline
semantics are not extended by a restart.

Upgrade note: when re-vendoring, drop these three hunks and check
whether upstream implemented `handleNextAlarm`/an `alarm()` handler —
if not, re-apply.

## Local patch: W2 P0-0 init single-flight + durable restart intent (binding.worker.js only)

`#resumeFromStorageIfNeeded` was single-flighted in W1-r5, but `init`
itself was not: its `isRunning` check→set window spans ~6 awaited
storage ops, and two call sites bypass the guard — the `create` RPC
(`stub.init`) and `attemptRestart` (`this.init`). Concurrent engine
executions then race the same step storage. Reproduced on real
workerd: two concurrent `restart()` calls interleave the two bare
inits and collide on `priority_queue`'s `UNIQUE(action, entryType,
hash)` — the instance ends `errored` with
`SQLITE_CONSTRAINT_UNIQUE` (see
`vrt-host/tests/do_w1r4_workerd.rs::restart_during_resume_does_not_double_run`).

A second, subtler defect: `userTriggeredRestart` aborts the actor to
quiesce, then `attemptRestart` runs on a fresh actor — but
`engineAbortController`/`pauseController` aborts cannot stop a
resumed run (a `step.do` callback races only its own timeout, never
the engine abort signal), so a resume that slipped in first would
re-run user steps to completion on wiped storage.

The patch makes restart intent durable and every `init` entry point
claim-participating:

- `RESTART_PENDING_KEY` is persisted by `userTriggeredRestart`
  *before* `ctx.abort` evicts the actor — the intent survives the
  eviction.
- `#resumeFromStorageIfNeeded` routes a persisted flag to
  `attemptRestart` instead of `init`, so the fresh actor's load-resume
  can never re-launch the killed run.
- `attemptRestart` is deduped (`#attemptRestartPromise`) and installs
  the `#initClaim` itself: join any in-flight claim → consume the flag
  (no flag → early return, so a restart RPC racing the resume-route
  restart is a no-op) → wipe → `#initInner` — all inside one claim.
- `init` is a claim shell: concurrent callers join the in-flight
  `#initInner` (`#initClaim`); the join/install loop has no await
  between re-check and assignment, so it is atomic on the
  single-threaded actor. `create`, resume and restart can never
  produce two `USER_WORKFLOW.run` invocations.
- `#initInner` re-checks `RESTART_PENDING_KEY` immediately before
  `isRunning = !0` — the last-moment gate for an intent persisted
  while this init was restoring state. Terminal/paused early-returns
  are unchanged.
- `changeInstanceStatus("restart")` coalesces on the flag: when an
  intent is already persisted the call returns early instead of
  aborting — a second `ctx.abort` would evict the actor running the
  first restart's claim, and the second RPC's transport error would
  escape `isUserTriggeredRestart` upstream. First restart's `from`
  wins; the joining caller's `attemptRestart` dedupes.
- `WorkflowHandle.restart` retries each RPC leg once on a fresh stub:
  a concurrent restart can leave the previous RPC in the abort dead
  zone (transport error, not the user-restart reason). Both legs are
  idempotent post-patch; a real error repeats and propagates.

worker semantics relied on: none new — the flag is ordinary DO KV,
consumed inside the claim; idempotent across aborts.

Upgrade note: keep all seven hunks; if upstream adds init-level
concurrency control (or aborts step callbacks on engineAbort), verify
the regression test still passes and drop ours.

## Upgrade process

1. Pick the miniflare release whose paired workerd is `<=` the pinned
   VRT workerd and closest to it (APIs the engine relies on must exist
   in our pin; newer engines may assume newer workerd).
2. Copy `dist/src/workers/workflows/binding.worker.js`,
   `wrapped-binding.worker.js` and `dist/src/workers/shared/
   zod.worker.js` over these files. If a newer bundle adds further
   `miniflare:` / `node:` imports, vendor the upstream targets in the
   same way — the `import_closure` test fails on any unregistered
   specifier.
3. Recompute `SHA256SUMS` (`shasum -a 256 *.js > SHA256SUMS`) and update
   the version fields above.
4. Re-run `cargo test -p vrt-config` (sha + capnp emit tests) and the
   `real_workerd_workflow` smoke with `VRT_WORKERD_BIN`.
