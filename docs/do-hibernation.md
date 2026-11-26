# Durable Object hibernation: staged design

Status: alarm index + alarm wake implemented (opt-in via the cell controller).
DO tenants without a fresh safe gate still stay resident.

## Current blocker

`vrt-host` scalps idle tenants to zero, but a worker declaring DO classes (or
workflows) is marked `StructuralDurableObject` and never sleeps. A DO alarm
cannot wake the host process, and an open WebSocket holds the in-flight guard.
Keeping the tenant resident is the current fail-safe.

## Goal

Let idle DO tenants (including workflow engines) sleep without losing alarms,
SQLite state, or WebSocket sessions, and wake them from:
- HTTP ingress,
- cron/queue dispatch,
- DO alarms,
- WebSocket frames.

## Required primitives

1. **Persistent alarm index — implemented** in the cell-controller: the
   earliest alarm per tenant/actor, refreshed from the host's read-only
   `_cf_METADATA` alarm scan and stored in `do-alarm-index.json` in the cell
   state directory. Classes whose scan failed are recorded as unknown and are
   never considered sleepable. See `cell-controller/src/alarm_index.rs` and
   the host `POST /v1/introspect/alarms` endpoint.
2. **Wake RPC — implemented** as `POST /v1/do-alarm/wake`. The pod starts the
   tenant through the shared `WakeFlight`; workerd itself loads
   `metadata.sqlite:_cf_ALARM` and fires overdue alarms. The controller
   consumes `AlarmIndex::due` every 5s and re-scans the class after a wake.
3. **DO storage quiescence — partially implemented**: the waitUntil probe now
   also counts active `alarm()` handlers (`pending + alarms`), so a handler that
   already cleared its alarm but is still running blocks sleep. Open sockets and
   storage transactions still keep the tenant resident.
4. **WebSocket detach/re-attach**: either reject sleep with open sockets
   (stage 1) or persist session metadata and reattach on wake (stage 2).
5. **Workflow engine special case**: workflow DOs must wake on alarms/ops, and
   terminal retention alarms must survive sleep.

## Stages

1. **Sleep eligibility — implemented behind the gate**: the controller pushes
   `POST /v1/do-alarm/gate` (`safe` + TTL) from the alarm index. A class must
   have been scanned since the controller started, be free of unknown/truncated
   reads and have no alarm inside the 60s lead window. A missing/expired gate
   keeps the tenant resident.
2. **Alarm wake**: persist earliest alarm, wake via cell-controller, re-arm.
3. **WebSocket hibernation — implemented, opt-in.** With
   `ingress.websocket.hibernation = true`, the ingress holds the client socket
   while workerd stops, releases the in-flight guard, buffers client frames up
   to `hibernationReplayBytes`, and on the next frame wakes the tenant and
   reattaches through a fresh upstream handshake. Overflow refuses the sleep
   (fail safe). Server frames emitted during the stop window are not buffered.
   The replay/detach state machine is unit-tested and
   `vrt-host/tests/s2z_hibernate_workerd.rs` covers a real-workerd
   sleep → client-frame reattach round trip.
4. **Automatic policy — implemented, opt-in** via `[scaleToZero.auto]`
   (`vrt-host/src/config.rs`, mechanisms in `vrt-host/src/hibernate.rs`):
   - **Adaptive idle window**: `AutoState` doubles the window after a
     sleep shorter than `prematureWakeMs` (the wake cost outran the
     residency saving) and halves it after a sleep at least
     `soundSleepMs` (deeply idle — could have slept sooner), bounded by
     `idleMs` (floor — the policy never sleeps a tenant sooner than
     declared) and `ceilingMs`.
   - **Wake budget**: `wakeBudget` successful wakes inside
     `wakeBudgetWindowMs` exhaust the tenant's hibernation budget — the
     policy then DEFERS sleep until enough counted wakes age out. The
     budget denies sleep, never traffic: a thrashing tenant simply
     stays resident.
   - **Max sleep**: `maxSleepMs` bounds how long a tenant may stay
     hibernated; at expiry the actor joins a `MaxSleep` wake flight
     through the normal coordinator path so spec/alarm-index state
     refreshes.
   - **Metrics**: `vrt_host_tenant_idle_window_seconds` (adapted
     window), `vrt_host_tenant_wake_budget` (remaining tokens, Inf =
     unlimited), `vrt_host_s2z_budget_deferred_total` (budget-denied
     evaluations); max-sleep wakes surface as
     `vrt_host_wake_total{source="max-sleep"}`.

   All knobs are bounds-checked at startup (fail closed); absent
   `[scaleToZero.auto]` keeps the fixed `idleMs` policy byte-identical.

## Invariants

- Never sleep with an unacknowledged alarm or an open storage transaction.
- Wake is idempotent; a failed wake leaves the tenant asleep/retryable, never
  half-serving.
- State lives in the existing DO SQLite layout; no second storage path.

## Tests

- Alarm during sleep fires after wake exactly once.
- HTTP/cron/queue wake races with alarm wake (single generation).
- Open WebSocket prevents sleep in stage 1.
- Workflow retention alarm still deletes state after sleep/wake.
