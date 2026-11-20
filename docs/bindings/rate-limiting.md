# Rate limiting

Workers-style `[[ratelimits]]` bindings — an in-process fixed-window
limiter inside the `facade-ratelimit` system worker.

**Status: Partial**

## Wrangler configuration

```toml
[[ratelimits]]
name = "LIMITER"
namespace_id = "1001"
simple = { limit = 1500, period = 60 }
```

- `name` — `env.<name>` binding name (required).
- `namespace_id` — a decimal-digit string of 1–32 digits, kept a string
  on the wire like Cloudflare (required).
- `simple.limit` — calls per period, 1–1,000,000 (required).
- `simple.period` — window length, 10 or 60 seconds only (required).
- At most 16 `[[ratelimits]]` entries per worker.

## Where it runs

- The tenant binding is a plain service binding to `facade-ratelimit`'s
  `RateLimiter` entrypoint; `env.LIMITER.limit({ key })` is a
  `WorkerEntrypoint` RPC (`facade/facade-ratelimit.ts`).
- Identity and limits come only from host-injected `ctx.props` — a
  tenant can pass nothing but `key`. The counter scope is tenant +
  worker + `namespace_id`, so two tenants or workers never share a
  counter.

## Backends

None — counters live in the facade system worker's process memory
(`RateLimitStore`), fixed-window semantics.

## Known differences from Cloudflare

- **Approximate and process-local.** On Cloudflare the counter is
  globally distributed and strongly consistent; here it is per tenant
  workerd process — a restarted process resets the window, and multiple
  cells each enforce their own share of the limit.
- `simple.period` accepts only 10 or 60 (the same rule Cloudflare
  documents); everything else fails at deploy.
- `key` semantics: the limiter partitions on `key` inside the tenant's
  counter scope; `limit({ key })` returns `{ success }` like
  Cloudflare's binding.

## Unsupported keys and error shape

- `ratelimits[]` accepts only `name`, `namespace_id`, `simple` — e.g.
  `ratelimits[0].extra` → `VRT_CONFIG_UNSUPPORTED_FIELD`.
- `simple` accepts only `limit` and `period` — e.g.
  `ratelimits[0].simple.burst` → `VRT_CONFIG_UNSUPPORTED_FIELD`.
- Non-digit / numeric / oversized `namespace_id`, `limit` outside
  1–1,000,000, `period` outside {10, 60} → `VRT_CONFIG_INVALID`.

## Examples and tests

- `facade/test/ratelimit.test.mjs` — facade rate-limit contract tests.
- `vrt-local/tests/local_ops.rs` — standalone Wrangler projection into
  the signed rate limit binding.
- `shared/vrt-project-config-vectors.json` — golden vectors covering
  `ratelimits` normalization and every rejected field/bounds case.

## Changelog

- 2026-11-20: Standalone Wrangler projection carries rate-limit settings into the signed desired state.
