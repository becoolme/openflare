# Hyperdrive

Hyperdrive-compatible Postgres access: a `postgres://` connection string
visible to the worker, routed through the egress layer to a loopback
proxy — real credentials never enter the isolate.

**Status: Partial**

## Wrangler configuration

```toml
[[hyperdrive]]
binding = "DB"
database_tenant = "my-db"   # required — a vrt database tenant name
id = "cf-hyperdrive-id"     # accepted, never used as an identity
```

`database_tenant` is an OpenFlare extension (not a Cloudflare key): it
names a `vrt` DatabaseTenant resolved server-side — lowercase letters,
digits, `-`/`_`, ≤ 63 chars.

## Where it runs

- The worker sees a `json` binding
  `{connectionString, host, port, user, password, database}` whose host
  is a platform-minted unresolvable `hd<hex24>.hyperdrive.vrt.internal`
  name (`vrt-config/src/capnp.rs`, MIG-15).
- The tenant's egress policy gains the synthetic host as an internal TCP
  target; `connect()`/`postgres` drivers land on `vrt-provider`'s
  `pg_proxy` listener (loopback only — `pgproxy.listen` must be a
  loopback address or config load fails).
- `vrt-provider` proxies the Postgres wire protocol to the real upstream
  using the credential payload held under `credentialRef`
  (`vrtpgcred:<id>` in `secrets/pg-credentials.json`) — the isolate only
  ever sees placeholder credentials.

## Backends

Postgres upstreams only, through `vrt-provider`'s `pg_proxy`. Per-binding
session and idle limits are configured in `[pg_proxy]`
(`connectTimeoutMs`, `idleTimeoutMs` 90 s, `maxSessionMs` 1 h,
`maxSessions` 256, `maxSessionsPerBinding` 8 — defaults in
`vrt-provider/src/config.rs`).

The related `[[pg]]` binding (vrt-native) exposes a Neon-compatible
query surface — `env.DB.query/.execute/.transaction` plus a `fetch`
adapter — over `facade-pg` and the provider's
`/internal/bindings/v1/pg/{bindingId}/sql` route. `env.DB.connect()` is
explicitly `BINDING_CAPABILITY_UNSUPPORTED`.

## Known differences from Cloudflare

- `database_tenant` is required and is an OpenFlare key — Cloudflare's
  account `id` is accepted and type-checked but never used as an
  identity (the server mints `vrthd:<tenant>:<name>` binding ids).
- `localConnectionString` and `remote` are dev-only keys accepted and
  dropped.
- Postgres wire protocol only — no MySQL.
- Sessions are bounded by `[pg_proxy]` caps and torn down at
  `maxSessionMs` regardless of activity; there is no connection pooling
  across hosts (the proxy is per-instance loopback).
- Credentials and rotation live entirely provider-side; a worker sees a
  static placeholder connection string.

## Unsupported keys and error shape

`hyperdrive[]` accepts `binding`, `id`, `database_tenant`,
`localConnectionString`, `remote` — anything else (e.g.
`hyperdrive_id`) → `VRT_CONFIG_UNSUPPORTED_FIELD`. A missing or
malformed `database_tenant` → `VRT_CONFIG_INVALID`.

## Examples and tests

- `shared/vrt-project-config-vectors.json` — golden vectors covering
  `hyperdrive` normalization (`hyperdrive binds by database_tenant`,
  `hyperdrive external id is never identity`, rejected cases).
- `vrt-host/tests/common/hyperdrive.rs` — e2e harness for Hyperdrive
  paths.
- `facade/test/pg.test.mjs` — `pg` facade contract tests.
