export const VECTORIZE_PROVIDER_PREFIX = "/internal/bindings/v1/vectorize/";
export const VECTORIZE_POST_OPS = [
    "query",
    "insert",
    "upsert",
    "getByIds",
    "deleteByIds"
];
export const VECTORIZE_GET_OPS = [
    "info"
];
export const VECTORIZE_BODY_MAX_BYTES = 8 * 1024 * 1024;
export const VECTORIZE_DRAIN_MAX_BYTES = 16 * 1024 * 1024;
export const VECTORIZE_BACKEND_TIMEOUT_MS = 15_000;
export const VECTORIZE_STRIP_HEADER_PREFIX = "x-vrt-";
export const OPEN_COMPUTE_PREFIX = "x-open-compute-";
