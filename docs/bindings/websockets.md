# WebSockets

Client WebSockets upgraded at the host ingress and proxied to `fetch()`
or a Durable Object; the full contract lives in
[`docs/ingress-websocket.md`](../ingress-websocket.md).

**Status: Partial**

## Wrangler configuration

No wrangler key — `WebSocketPair` and the `webSocket` request path need
no declaration. Tuning is host-side in `vrt-host.toml`
(`vrt-host/src/config.rs`, `WebsocketConfig`; every key optional, the
whole section may be absent):

```toml
[ingress.websocket]
idleTimeoutSecs = 300          # 1..=3600
maxDurationSecs = 14400        # 1..=86400
maxSessions = 4096             # 1..=65536
maxSessionsPerTenant = 256     # 1..=maxSessions
```

Out-of-range values and unknown keys are rejected at config load;
nothing is silently clamped.

## Where it runs

- `vrt-host` ingress (`vrt-host/src/ingress.rs`) terminates the upgrade
  and opens the workerd `websocket` connector on the worker's socket.
- Session lifecycle lives in the ingress session registry; Durable
  Object sessions reach the actor through the same proxy path.

## Backends

None — pure HTTP/1.1 upgrade passthrough to workers and DO actors.

## Known differences from Cloudflare

- HTTP/1.1 upgrades only — no HTTP/2 extended CONNECT.
- Session caps: 300 s idle timeout, 4 h max session duration, 4096
  concurrent sessions per ingress, 256 per tenant by default; a full
  ingress or tenant cap answers `503` with platform code `ws_capacity`
  (`docs/log-codes.md`), before the worker is woken or contacted.
- Open sessions hold the tenant's in-flight guard — a WebSocket keeps a
  scale-to-zero tenant awake; Cloudflare-style WebSocket hibernation
  (evicting the DO while sockets stay open) is not emulated.
- The per-tenant cap counts sessions on one ingress only — it is not
  shared across hosts.
- See [`docs/ingress-websocket.md`](../ingress-websocket.md) for the
  exact upgrade contract, header rewriting and metrics.

## Unsupported keys and error shape

Not a wrangler binding — nothing to reject. Host `[ingress.websocket]`
values are typed and range-checked at config load
(`deny_unknown_fields`).

## Examples and tests

- `docs/ingress-websocket.md` — full contract.
- `vrt-host/tests/ingress.rs` — ingress WebSocket/session tests.
