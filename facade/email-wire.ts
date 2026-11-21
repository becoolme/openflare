// Shared typed wire between `facade-email` (the send_email wrapped
// binding's inner `fetcher` service) and the provider's email route —
// BOTH sides import these constants so the contract cannot silently
// fork (same discipline as k2-wire.ts / queue-wire.ts).
// Provider mirror: vrt-provider::server::parse_email_path.

/// Provider route prefix: `/internal/bindings/v1/email/{bindingId}/{op}`.
export const EMAIL_PROVIDER_PREFIX = "/internal/bindings/v1/email/";

/// Ops accepted on the last path segment — the
/// `cloudflare-internal:send-email` shim emits `/send`.
export const EMAIL_OPS = ["send"] as const;
export type EmailOp = (typeof EMAIL_OPS)[number];

/// Host-controlled trusted context prefixes — a tenant must never
/// inject them; facade-email strips ALL of them inbound (S4 + T34d).
export const EMAIL_STRIP_HEADER_PREFIX = "x-vrt-";
export const OPEN_COMPUTE_PREFIX = "x-open-compute-";

/// Inbound body cap. CF: total message ≤25MiB; base64 attachments
/// inflate ~4/3 plus JSON field overhead — 36MiB accepts every legal
/// message and bounds the rest here (mirrors provider
/// EMAIL_MAX_BODY_BYTES).
export const EMAIL_BODY_MAX_BYTES = 36_000_000;

/// Garbage-stream bound for the drain-before-return invariant.
export const EMAIL_DRAIN_MAX_BYTES = EMAIL_BODY_MAX_BYTES * 2;

/// facade → provider call deadline (ms). Upstream adapters carry their
/// own 15s timeout; the facade bound sits just above it so a wedged
/// adapter still surfaces as a typed error, not a hang.
export const EMAIL_BACKEND_TIMEOUT_MS = 20_000;
