// Shared constants for the Vectorize wire (facade-vectorize).
//
// The upstream `cloudflare-internal:vectorize-api` shim posts the v2
// (GA) surface at these paths on the inner `fetcher`:
//   GET  /info
//   POST /query        {vector|vectorId, topK?, namespace?, filter?,
//                       returnValues?, returnMetadata?}
//   POST /insert       {vectors:[{id,values,metadata?,namespace?}]}
//   POST /upsert       same
//   POST /getByIds     {ids:[...]}
//   POST /deleteByIds  {ids:[...]}
// Errors are non-200 + JSON `{code:number, error:string}` — the shim
// raises `<OP>_ERROR (code = N): <error>`.

export const VECTORIZE_PROVIDER_PREFIX = "/internal/bindings/v1/vectorize/";

export const VECTORIZE_POST_OPS = [
  "query",
  "insert",
  "upsert",
  "getByIds",
  "deleteByIds",
] as const;

export const VECTORIZE_GET_OPS = ["info"] as const;

/// Body cap: CF's own insert bound is 1000 vectors/batch — 8MiB covers
/// 1000 × 1536 f32 as JSON with wide margin; larger posts are malformed.
export const VECTORIZE_BODY_MAX_BYTES = 8 * 1024 * 1024;
/// Drain bound for an oversize body — bounded like k2's drain cap.
export const VECTORIZE_DRAIN_MAX_BYTES = 16 * 1024 * 1024;

export const VECTORIZE_BACKEND_TIMEOUT_MS = 15_000;

/// Trusted-context prefixes that must never ride into the provider —
/// inbound copies are user-controlled forgery (T34d).
export const VECTORIZE_STRIP_HEADER_PREFIX = "x-vrt-";
export const OPEN_COMPUTE_PREFIX = "x-open-compute-";
