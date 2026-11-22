export const EMAIL_PROVIDER_PREFIX = "/internal/bindings/v1/email/";
export const EMAIL_OPS = [
    "send"
];
export const EMAIL_STRIP_HEADER_PREFIX = "x-vrt-";
export const OPEN_COMPUTE_PREFIX = "x-open-compute-";
export const EMAIL_BODY_MAX_BYTES = 36_000_000;
export const EMAIL_DRAIN_MAX_BYTES = EMAIL_BODY_MAX_BYTES * 2;
export const EMAIL_BACKEND_TIMEOUT_MS = 20_000;
