# WebSocket hibernation spike

Status: **spike only** — nothing here is wired into `vrt-host`.

The production ingress tunnels raw WebSocket bytes to the tenant workerd
process and holds the in-flight guard for the whole session. Workerd has no API
to hand an existing client socket to a new process, so the tenant cannot sleep
with open sockets today (fail safe: `docs/ingress-websocket.md`).

## Proposed architecture

1. The ingress keeps the client WebSocket (and its TLS/TCP connection) while
   the tenant generation stops. It parses frames only enough to enforce the
   replay bound; the payload stays opaque.
2. On detach the host stores a bounded replay window and a signed resume token
   in the tenant/DO session metadata. If the window cannot fit, sleep is
   refused (tenant stays resident), never silently truncated.
3. The cell-controller wake path starts a new generation. The ingress performs
   an internal resume handshake with the new workerd generation, then replays
   buffered client frames and any unacked server frames in order.
4. Application state still lives in DO SQLite; only transport frames live in
   the replay window. The session token is scoped to `(tenant, session)` and
   single-use.

## What this spike proves

`state.mjs` is the pure detach/reattach state machine:

```bash
node --test spike/ws-hibernation/state.test.mjs
```

It covers ordered replay, the bounded-buffer fail-safe, the detach gate
(including pending client bytes), single-use resume tokens and closed-session
handling. It deliberately has no sockets and no production imports.

## Next step before production

Build the ingress proxy around this machine, add the workerd-side resume
handshake, and define the DO storage schema for session metadata. Until then
open sessions keep the tenant resident.
