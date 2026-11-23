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

Postgres upstreams only, through `vrt-provider`'s `pg_proxy` listener.
The listener plus per-session limits are configured in `[pgProxy]`
(`connectTimeoutMs`, `idleTimeoutMs` 90 s, `maxSessionMs` 1 h,
`maxSessions` 256, `maxSessionsPerBinding` 8 — defaults in
`vrt-provider/src/config.rs`).

### Session pooling

`[hyperdrive.pool]` turns on a proxy-side **session pool**: when a
client session ends, its backend connection is reset and parked, and the
next session of the same `(tenant, upstream endpoint, user, database)`
continues on it. Without the section every session dials its own
connection (the pre-r3 shape).

```toml
[pgProxy]
listen = "127.0.0.1:19543"        # loopback only — enforced at startup

[hyperdrive.pool]
maxConnections = 8                # live backend sockets, per process
acquireTimeoutMs = 5000           # bounded queue, then a typed denial
idleTimeoutMs = 30000             # reclaim unused sockets
maxLifetimeMs = 1800000           # retire a socket even under steady use
resetOnReturn = true              # DISCARD ALL before reuse
```

Every value is validated at load: `maxConnections` must be 1..=1024,
`acquireTimeoutMs`/`idleTimeoutMs` 100..=300000 ms, `maxLifetimeMs`
1000..=2592000000 ms. A zero, negative, out-of-range or unknown key fails
startup (`VRT_CONFIG_INVALID` or a TOML decode error) instead of falling
back to a default, and `[hyperdrive]` without `[pgProxy]` (or `[pgProxy]`
without `[pg]`) is refused.

Semantics:

- **Session pooling only.** One client session owns one backend
  connection for its whole life. Statements are never multiplexed over a
  shared connection between sessions (no transaction pooling), and there
  is no query or result cache.
- **`maxConnections` is per process.** Standalone runs one
  `vrt-provider`, so there it is the global ceiling. With `N` replicas
  the cluster-wide ceiling is `N x maxConnections`, i.e. PostgreSQL sees
  up to that many backend sessions from the cell.
- **Over the cap is queued, then denied.** A session that cannot get a
  connection waits at most `acquireTimeoutMs` — an independent deadline,
  not charged against `connectTimeoutMs` — and is then refused with
  SQLSTATE `53300` and `hyperdrive: connection limit reached`. The waiter
  queue is bounded: it never waits indefinitely.
- **Reuse needs a clean boundary.** A connection returns to the pool only
  when the session ended exactly on a `ReadyForQuery` with no client
  request outstanding, the checkout probe (`SELECT 1`) still succeeds,
  and the reset succeeded. An open transaction is rolled back first; then
  `DISCARD ALL` runs (`resetOnReturn`); any failure closes the socket
  instead of handing dirty state on. `idleTimeoutMs` reclaims unused
  sockets and `maxLifetimeMs` retires them even under steady use.
- **No cross-tenant reuse.** The tenant is part of the key, so a
  different tenant, database or role always gets its own connection, and
  `DISCARD ALL` clears temp tables, GUCs and prepared statements between
  sessions of the same key.
- **Client `Terminate` is not forwarded.** The proxy ends the client
  session and keeps the backend socket for the next session of that key —
  that is what makes reuse observable in `pg_backend_pid()`.
- The pool is in-memory: a process restart drops every backend
  connection. Credential rotation does not flush parked connections; they
  keep running as the role they authenticated as until `maxLifetimeMs`
  retires them.
- Idle connections of a tenant whose binding leaves desired state are
  closed by the same sweeper interval as live sessions (5 s).

Pool metrics (internal metrics listener):
`vrt_provider_pgproxy_pool_in_use`, `vrt_provider_pgproxy_pool_idle`,
`vrt_provider_pgproxy_pool_acquire_wait_ms`,
`vrt_provider_pgproxy_pool_acquire_timeout_total`,
`vrt_provider_pgproxy_pool_reset_failed_total`,
`vrt_provider_pgproxy_pool_reused_total`,
`vrt_provider_pgproxy_pool_opened_total`,
`vrt_provider_pgproxy_pool_closed_total{reason}` with reason in
`broken|idle|lifetime|tenant_revoked`.

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
- Sessions are bounded by `[pgProxy]` caps and torn down at
  `maxSessionMs` regardless of activity.
- With `[hyperdrive.pool]` the proxy pools backend connections
  (`maxConnections` per process). It is session pooling only — no
  transaction pooling, no query cache — so a worker that opens many
  short sessions still consumes one backend connection at a time per
  concurrent session.
- Credentials and rotation live entirely provider-side; a worker sees a
  static placeholder connection string.

## Unsupported keys and error shape

`hyperdrive[]` accepts `binding`, `id`, `database_tenant`,
`localConnectionString`, `remote` — anything else (e.g.
`hyperdrive_id`) → `VRT_CONFIG_UNSUPPORTED_FIELD`. A missing or
malformed `database_tenant` → `VRT_CONFIG_INVALID`.

Provider-side pool exhaustion is surfaced over the wire, not as a
generic failure: `SQLSTATE 53300` with the message
`hyperdrive: connection limit reached` (`PG_CONNECTION_LIMIT`), so a
driver classifies it as a capacity error instead of a syntax or protocol
error.

## Examples and tests

- `shared/vrt-project-config-vectors.json` — golden vectors covering
  `hyperdrive` normalization (`hyperdrive binds by database_tenant`,
  `hyperdrive external id is never identity`, rejected cases).
- `vrt-local/tests/local_ops.rs` — standalone Wrangler projection and
  server-minted binding identity/credential reference.
- `vrt-host/tests/common/hyperdrive.rs` — e2e harness for Hyperdrive
  paths.
- `vrt-provider/tests/pgproxy_pool.rs` — session pool against a real
  PostgreSQL 16: backend reuse via `pg_backend_pid()`, `DISCARD ALL`
  state reset, `maxConnections = 1` queue timeout and tenant isolation.
- `vrt-provider/tests/startup_config_rv72.rs` — `[hyperdrive.pool]`
  bounds, unknown keys and orphan sections fail closed.
- `facade/test/pg.test.mjs` — `pg` facade contract tests.

## Changelog

- 2026-11-23: Per-process session pool (`[hyperdrive.pool]`, MIG-15 r3):
  `maxConnections` with a bounded acquire queue, checkout probe, rollback +
  `DISCARD ALL` on return, idle/lifetime reclamation, tenant-keyed reuse,
  client `Terminate` interception, pool metrics and a typed `53300` denial.
  Session pooling only — still no transaction pooling or query cache.
- 2026-11-20: Standalone Wrangler projection carries Hyperdrive into the signed desired state.
