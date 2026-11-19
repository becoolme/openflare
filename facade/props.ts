// Shared ctx.props → x-open-compute-* identity plumbing for vrt facade system
// workers. Identity headers are built ONLY from host-injected props; nothing a
// tenant can supply reaches them. Vendored transports validate a subset of the
// props themselves; this layer applies the full ResourceBindingProps contract
// and fails closed before any request is issued.
import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";

const MAX_PROPS_STRING = 512;
// Header values must never carry control characters; Headers.set() already
// throws for most of them, but rejecting here keeps failure typed.
const CONTROL_CHAR_RE = /[\x00-\x1f\x7f]/;
// bindingId is interpolated into the URL path segment
// (/internal/bindings/v1/<product>/<bindingId>/<op>); path and delimiter
// characters would let a poisoned props object rewrite the request line.
const BINDING_ID_SAFE_RE = /^[^\x00-\x20\/?#%]+$/;

export interface ResourceProps {
  bindingId: string;
  versionId: string;
  workerId: string;
  instanceId: string;
  descriptorSha256: string;
  namespaceResourceId: string;
  resourceSpecGeneration: number;
  permissions: { read: boolean; write: boolean };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function invalid(): never {
  throw bindingError("BINDING_PROTOCOL_ERROR");
}

/** Strictly narrow host-injected ctx.props; any deviation fails closed. */
export function resourceProps(props: unknown): ResourceProps {
  if (!isRecord(props)) invalid();
  for (const field of [
    "bindingId",
    "versionId",
    "workerId",
    "instanceId",
    "descriptorSha256",
    "namespaceResourceId",
  ] as const) {
    const value = props[field];
    if (
      typeof value !== "string" ||
      value.length === 0 ||
      value.length > MAX_PROPS_STRING
    ) {
      invalid();
    }
  }
  for (const field of [
    "bindingId",
    "versionId",
    "workerId",
    "instanceId",
    "descriptorSha256",
    "namespaceResourceId",
  ] as const) {
    if (CONTROL_CHAR_RE.test(props[field] as string)) invalid();
  }
  if (!BINDING_ID_SAFE_RE.test(props.bindingId as string)) invalid();
  if (!/^[0-9a-f]{64}$/.test(props.descriptorSha256 as string)) invalid();
  if (
    !Number.isSafeInteger(props.resourceSpecGeneration) ||
    (props.resourceSpecGeneration as number) < 1
  ) {
    invalid();
  }
  if (
    !isRecord(props.permissions) ||
    typeof props.permissions.read !== "boolean" ||
    typeof props.permissions.write !== "boolean"
  ) {
    invalid();
  }
  return props as unknown as ResourceProps;
}

export function requirePermission(
  props: ResourceProps,
  permission: "read" | "write",
): void {
  if (!props.permissions[permission]) {
    throw bindingError("BINDING_PERMISSION_DENIED");
  }
}

/** Headers the provider authorizes against; values only from validated props. */
export function identityHeaders(props: ResourceProps): Record<string, string> {
  return {
    "x-open-compute-binding-id": props.bindingId,
    "x-open-compute-version-id": props.versionId,
    "x-open-compute-worker-id": props.workerId,
    "x-open-compute-descriptor-sha256": props.descriptorSha256,
    "x-open-compute-namespace-resource-id": props.namespaceResourceId,
    "x-open-compute-resource-spec-generation": String(
      props.resourceSpecGeneration,
    ),
  };
}

interface FetchLike {
  fetch(input: unknown, init?: { headers?: unknown; [key: string]: unknown }): unknown;
}

/**
 * Wrap env.BINDING_BACKEND so every outbound call carries the full vrt
 * identity header set (vendored transports only send version/digest/request-id
 * upstream). Identity headers are applied after the caller's own headers, so a
 * caller-supplied `x-open-compute-*` value can never win. Props are validated
 * once at binding construction — a worker with forged or missing props fails
 * before its first request.
 */
export function wrapBindingBackend(
  backend: FetchLike | null | undefined,
  rawProps: unknown,
): FetchLike {
  const props = resourceProps(rawProps);
  if (!backend || typeof backend.fetch !== "function") {
    throw bindingError("BINDING_PROTOCOL_ERROR");
  }
  const extra = identityHeaders(props);
  return {
    fetch(input: unknown, init?: { headers?: unknown; [key: string]: unknown }) {
      const headers = new Headers(
        input instanceof Request ? input.headers : undefined,
      );
      if (init && init.headers !== undefined) {
        new Headers(init.headers as HeadersInit).forEach((value, key) =>
          headers.set(key, value),
        );
      }
      for (const [key, value] of Object.entries(extra)) {
        headers.set(key, value);
      }
      return backend.fetch(input, { ...(init ?? {}), headers });
    },
  };
}

/** Wrap env for a WorkerEntrypoint subclass, replacing BINDING_BACKEND. */
export function envWithBackend(env: unknown, rawProps: unknown): unknown {
  const base = (env ?? {}) as Record<string, unknown>;
  const wrapped = wrapBindingBackend(
    base.BINDING_BACKEND as FetchLike | null | undefined,
    rawProps,
  );
  return Object.assign(Object.create(Object.getPrototypeOf(base)), base, {
    BINDING_BACKEND: wrapped,
  });
}
