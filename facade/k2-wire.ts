// Shared typed wire between `facade-k2` (the K2 wrapped binding's inner
// `fetcher` service) and the provider's k2 produce route — BOTH sides
// import these constants so the contract cannot silently fork (same
// discipline as queue-wire.ts).
// Provider mirror: vrt-provider::server::parse_k2_path.

/// Provider route prefix: `/internal/bindings/v1/k2/{bindingId}/{op}`.
export const K2_PROVIDER_PREFIX = "/internal/bindings/v1/k2/";

/// Ops accepted on the last path segment of the K2 request (the
/// `cloudflare-internal:k2` shim emits `/send`).
export const K2_OPS = ["send"] as const;
export type K2Op = (typeof K2_OPS)[number];

/// Everything under this prefix is host-controlled trusted context —
/// a tenant must never inject it; facade-k2 strips ALL of them from
/// inbound requests before forwarding (S4 + T34 adversarial case).
export const K2_STRIP_HEADER_PREFIX = "x-vrt-";

/// The provider identity channel (`x-open-compute-binding-id` etc.) —
/// same rule: host-injected only, inbound copies are stripped (T34d).
export const OPEN_COMPUTE_PREFIX = "x-open-compute-";

/// Inbound body cap. CF K2: record ≤1MB, batch ≤5MB — JSON base64
/// inflates content by ~4/3 (≈6.7MB worst case) plus headers/field
/// overhead; 8MB accepts every legal batch and bounds the rest here.
export const K2_BODY_MAX_BYTES = 8_000_000;

/// Garbage-stream bound for the drain-before-return invariant — a
/// pathological source is abandoned via cancel() at this hard multiple.
export const K2_DRAIN_MAX_BYTES = K2_BODY_MAX_BYTES * 4;

/// facade → provider call deadline (ms). Above vrt-sql's 5s op budget
/// plus transport slack, below any tenant request budget.
export const K2_BACKEND_TIMEOUT_MS = 10_000;
