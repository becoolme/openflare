import { bindingError } from "../third_party/open-compute/facade/loader/shared.js";
const MAX_PROPS_STRING = 512;
const CONTROL_CHAR_RE = /[\x00-\x1f\x7f]/;
const BINDING_ID_SAFE_RE = /^[^\x00-\x20\/?#%]+$/;
function isRecord(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function invalid() {
    throw bindingError("BINDING_PROTOCOL_ERROR");
}
export function resourceProps(props) {
    if (!isRecord(props)) invalid();
    for (const field of [
        "bindingId",
        "versionId",
        "workerId",
        "instanceId",
        "descriptorSha256",
        "namespaceResourceId"
    ]){
        const value = props[field];
        if (typeof value !== "string" || value.length === 0 || value.length > MAX_PROPS_STRING) {
            invalid();
        }
    }
    for (const field of [
        "bindingId",
        "versionId",
        "workerId",
        "instanceId",
        "descriptorSha256",
        "namespaceResourceId"
    ]){
        if (CONTROL_CHAR_RE.test(props[field])) invalid();
    }
    if (!BINDING_ID_SAFE_RE.test(props.bindingId)) invalid();
    if (!/^[0-9a-f]{64}$/.test(props.descriptorSha256)) invalid();
    if (!Number.isSafeInteger(props.resourceSpecGeneration) || props.resourceSpecGeneration < 1) {
        invalid();
    }
    if (!isRecord(props.permissions) || typeof props.permissions.read !== "boolean" || typeof props.permissions.write !== "boolean") {
        invalid();
    }
    return props;
}
export function requirePermission(props, permission) {
    if (!props.permissions[permission]) {
        throw bindingError("BINDING_PERMISSION_DENIED");
    }
}
export function identityHeaders(props) {
    return {
        "x-open-compute-binding-id": props.bindingId,
        "x-open-compute-version-id": props.versionId,
        "x-open-compute-worker-id": props.workerId,
        "x-open-compute-descriptor-sha256": props.descriptorSha256,
        "x-open-compute-namespace-resource-id": props.namespaceResourceId,
        "x-open-compute-resource-spec-generation": String(props.resourceSpecGeneration)
    };
}
export function wrapBindingBackend(backend, rawProps) {
    const props = resourceProps(rawProps);
    if (!backend || typeof backend.fetch !== "function") {
        throw bindingError("BINDING_PROTOCOL_ERROR");
    }
    const extra = identityHeaders(props);
    return {
        fetch (input, init) {
            const headers = new Headers(input instanceof Request ? input.headers : undefined);
            if (init && init.headers !== undefined) {
                new Headers(init.headers).forEach((value, key)=>headers.set(key, value));
            }
            for (const [key, value] of Object.entries(extra)){
                headers.set(key, value);
            }
            return backend.fetch(input, {
                ...init ?? {},
                headers
            });
        }
    };
}
export function envWithBackend(env, rawProps) {
    const base = env ?? {};
    const wrapped = wrapBindingBackend(base.BINDING_BACKEND, rawProps);
    return Object.assign(Object.create(Object.getPrototypeOf(base)), base, {
        BINDING_BACKEND: wrapped
    });
}
