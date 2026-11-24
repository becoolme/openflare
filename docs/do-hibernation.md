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
3. **DO storage quiescence**: no in-flight requests, no open WebSocket, no
   storage transaction; otherwise skip sleep.
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
3. **WebSocket hibernation**: detach sessions, bounded replay buffer,
   reattach; reject if a frame cannot be buffered.
4. **Automatic policy**: idle window, max sleep, wake budget, metrics.

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
