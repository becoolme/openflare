# WebSocket passthrough

The `vrt-host` HTTP ingress forwards WebSocket upgrades to workers. A worker
that answers a handshake with `101 Switching Protocols` (for example a Durable
Object using `WebSocketPair` and `acceptWebSocket`) reaches the client as a
`101`, followed by a bidirectional byte tunnel. The ingress never parses
WebSocket frames: it only relays bytes after the handshake.

## Detection

A request is treated as a WebSocket upgrade only if all of the following hold:

- HTTP/1.1 (HTTP/1.0 and HTTP/2 are not upgrades).
- Method is `GET`.
- The `Upgrade` header lists exactly one token, `websocket`, compared
  case-insensitively.
- The `Connection` header, across all header lines, contains an `upgrade`
  token, compared case-insensitively.
- The request has no body framing: no `Transfer-Encoding`, and no
  `Content-Length` other than `0`.

Anything else is a normal request. In particular a different `Upgrade`
protocol such as `h2c`, or a mixed list such as `websocket, h2c`, keeps having
its `Upgrade` and `Connection` headers stripped (fail closed). HTTP/2 extended
`CONNECT` is not supported and is handled as an ordinary request.

## Header handling

- For an upgrade, the ingress sends `Connection: Upgrade` and
  `Upgrade: websocket` upstream, replacing whatever the client sent.
  `Sec-WebSocket-*` headers pass through unchanged.
- The trust-boundary rules are the same as for every other request: client
  supplied `x-forwarded-*`, `cf-*` and `x-vrt-*` headers are removed and
  rewritten by the ingress.
- The `101` response keeps the handshake headers the worker produced
  (`Sec-WebSocket-Accept`, `Sec-WebSocket-Protocol`, `Sec-WebSocket-Extensions`).
  `Connection: Upgrade` and `Upgrade: websocket` are re-added by the ingress.
  A `101` without `Upgrade: websocket`, or a `101` to a request that was not
  an upgrade, is answered as an upstream error.
- A non-`101` response to an upgrade request (for example `403`) is passed
  through like any other response.

## Timeouts and limits

| Limit | Default | Behavior |
| --- | --- | --- |
| Handshake | the ingress `request_timeout` | Covers the request up to the `101` response, and the upgrade handover on both sides. It does not cut an established session. |
| Idle timeout | 300 s | No bytes in either direction for this long closes the session. |
| Maximum duration | 4 h | The session is closed after this long regardless of activity. |
| Concurrent sessions | 4096 per ingress | Above the cap the request is rejected with `503` and platform code `ws_capacity`, before the worker is woken or contacted. |
| Concurrent sessions per tenant | 256 | A tenant over its own cap is rejected the same way (`503`, `ws_capacity`), so one tenant cannot use up the ingress-wide cap. |

A new session reserves a slot against both caps before any wake or upstream
work. If either refuses, whatever was already reserved is released. The
per-tenant counts live in a table that drops a tenant's entry when its count
reaches zero, so the table is bounded by the number of tenants with open
sessions.

## Configuration

The limits are set in the `[ingress.websocket]` section of the host config.
Every key is optional and an absent section keeps the defaults above.

```toml
[ingress.websocket]
idleTimeoutSecs = 300          # 1..=3600
maxDurationSecs = 14400        # 1..=86400
maxSessions = 4096             # 1..=65536
maxSessionsPerTenant = 256     # 1..=maxSessions
hibernation = false            # hold detached sessions across tenant sleep
hibernationReplayBytes = 65536 # 1024..=1048576 bounded client-frame replay
```

Values outside these ranges, and unknown keys, are rejected when the config is
loaded; nothing is silently clamped. When only `maxSessions` is set below 256,
the per-tenant default becomes `maxSessions`.

Programmatically the same limits are set with
`IngressState::with_ws_limits(idle_timeout, max_duration, max_sessions,
max_sessions_per_tenant)`, which clamps zero values to the smallest valid value.

Each session holds the worker's in-flight guard for its whole lifetime. A
session therefore counts as an active request for drain and scale-to-zero
accounting, and the guard is released when the session ends for any reason.
If either side fails to complete its upgrade, a metric is recorded and the
connection is closed; the guard and the session slot are still released.

## Metrics

| Metric | Labels | Meaning |
| --- | --- | --- |
| `vrt_host_ws_sessions_total` | `worker_id`, `outcome` | Handshake outcomes: `upgraded`, `rejected_capacity` (ingress-wide cap), `rejected_tenant_capacity` (per-tenant cap), `rejected_upstream`, `client_upgrade_failed`, `upstream_upgrade_failed`. |
| `vrt_host_ws_session_end_total` | `worker_id`, `reason` | Why an upgraded session ended: `closed`, `error`, `idle_timeout`, `max_duration`. An abrupt peer reset is reported as `error`. |
| `vrt_host_ws_active_sessions` | none | Gauge of sessions currently open. |
| `vrt_host_ws_bytes_total` | `worker_id`, `dir` | Bytes relayed, `dir` is `client_to_worker` or `worker_to_client`. |

The request metric of an upgraded request is finished with status `101` when
the handshake completes, not when the session ends.

## Limitations

- A tenant with an open Durable Object WebSocket session is never idle: the
  session holds the in-flight guard, so the tenant is not hibernated or
  scaled to zero while it is open. Close idle sessions on the client or rely
  on the idle timeout.
- Cloudflare's hibernatable WebSocket eviction is not emulated by default. The
  worker process stays resident while the session is open. With
  `ingress.websocket.hibernation = true`, the ingress releases the tenant's
  in-flight guard when workerd stops, holds the client socket, buffers client
  frames up to `hibernationReplayBytes`, and on the next client frame wakes the
  tenant, performs a fresh upstream WebSocket handshake and replays those
  frames. Server frames emitted while the process was stopping are not
  buffered, so applications must tolerate a resume handshake; overflow refuses
  the sleep instead of truncating. The original request path/host is replayed,
  so the worker must resolve the same session from its URL/headers.
- When a deployment switches generations, the old generation is drained for a
  grace period. A session still open when the grace period expires is cut.
  Clients are expected to reconnect.
- Only HTTP/1.1 upgrades are supported.
- The per-tenant cap counts sessions on this ingress only. It is not shared
  across hosts.
