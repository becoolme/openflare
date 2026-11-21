// Shared typed wire between `facade-d1` (the wrapped binding's inner
// fetcher) and the provider's D1 route — BOTH sides import these
// constants so the contract cannot silently fork (T31f/F0 lesson).
// Provider mirror: vrt-provider::server::parse_d1_path.

/// Provider route prefix: `/internal/bindings/v1/d1/{bindingId}/{op}`.
export const D1_PROVIDER_PREFIX = "/internal/bindings/v1/d1/";

/// Ops accepted on the last path segment (d1-api emits these).
/// `query`/`execute` are the prepared-statement calls; `dump` is
/// `D1Database.dump()` (POST /dump).
export const D1_OPS = ["query", "execute", "dump"] as const;
export type D1Op = (typeof D1_OPS)[number];

/// Headers forwarded from the d1-api request to the provider — D1
/// session/bookmark semantics live in `x-cf-d1-*`.
export const D1_SESSION_HEADER_PREFIX = "x-cf-d1-";

/// Everything under this prefix is host-controlled trusted context —
/// a tenant must never inject it; facade-d1 strips ALL of them from
/// inbound requests before forwarding (S4 + T34 adversarial case).
export const D1_STRIP_HEADER_PREFIX = "x-vrt-";

/// The provider identity channel (`x-open-compute-binding-id` etc.) —
/// same rule: host-injected only, inbound copies are stripped (T34d).
export const OPEN_COMPUTE_PREFIX = "x-open-compute-";
