# Durable Object hibernation: staged design

Status: alarm index implemented; sleep eligibility and wake are not implemented
(DO tenants still stay resident).

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
2. **Wake RPC** from cell-controller to the pod supervisor: load the tenant
   generation, replay alarms, then allow traffic.
3. **DO storage quiescence**: no in-flight requests, no open WebSocket, no
   storage transaction; otherwise skip sleep.
4. **WebSocket detach/re-attach**: either reject sleep with open sockets
   (stage 1) or persist session metadata and reattach on wake (stage 2).
5. **Workflow engine special case**: workflow DOs must wake on alarms/ops, and
   terminal retention alarms must survive sleep.

## Stages

1. **Sleep eligibility**: allow DO tenants with no open sockets and an
   empty/inactive alarm set to scale to zero; prove no data loss.
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
