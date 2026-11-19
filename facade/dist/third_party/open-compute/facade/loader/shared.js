import { resolvePythonSnapshot } from "./python-snapshot.js";
import { assertSnapshot } from "./snapshot.js";
const SOURCE_PATH = "/internal/runtime/v1/versions/resolve";
let startupGeneration;
const assembling = new Map();
const WARM_RUNTIME_SNAPSHOTS = 1024;
export const WARM_RUNTIME_SNAPSHOT_BYTES = 32 * 1024 * 1024;
const warmRuntimeSnapshots = new Map();
const warmRuntimeSnapshotSizes = new Map();
let warmRuntimeSnapshotTotal = 0;
function warmSnapshotKey(envelope, scope) {
    if (scope !== "runtime" || envelope.routeGeneration === undefined) return null;
    return `${envelope.loaderKey}\0${envelope.expected}\0${envelope.routeGeneration}`;
}
export function systemRequestId() {
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    let timestamp = Date.now();
    for(let index = 5; index >= 0; index -= 1){
        bytes[index] = timestamp % 256;
        timestamp = Math.floor(timestamp / 256);
    }
    bytes[6] = bytes[6] & 0x0f | 0x70;
    bytes[8] = bytes[8] & 0x3f | 0x80;
    const hex = Array.from(bytes, (byte)=>byte.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
export const INTERNAL_HEADERS = Object.freeze([
    "x-open-compute-service-frame",
    "x-open-compute-service-websocket-handoffs",
    "x-open-compute-instance-id",
    "x-open-compute-worker-id",
    "x-open-compute-version-id",
    "x-open-compute-loader-key",
    "x-open-compute-execution-started",
    "x-open-compute-asset-representation-length",
    "x-open-compute-worker-code-sha256",
    "x-open-compute-entrypoint",
    "x-open-compute-original-method",
    "x-open-compute-original-url",
    "x-open-compute-route-generation",
    "x-open-compute-request-id",
    "x-open-compute-output-gate",
    "x-open-compute-resource-limit",
    "x-open-compute-binding-id",
    "x-open-compute-descriptor-sha256",
    "x-open-compute-namespace-resource-id",
    "x-open-compute-object-id",
    "x-open-compute-object-generation",
    "x-open-compute-class-name",
    "x-open-compute-asset-method",
    "x-open-compute-asset-url",
    "x-open-compute-do-method",
    "x-open-compute-do-url",
    "x-open-compute-do-operation",
    "x-open-compute-startup-generation",
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto"
]);
export function tenantGlobalOutbound(env, validation) {
    if (validation) return null;
    if (env.OUTBOUND_NETWORK == null) throw bindingError("VERSION_INVARIANT_VIOLATION");
    return env.OUTBOUND_NETWORK;
}
function workerCodeOptions(snapshot) {
    if (typeof snapshot.compatibilityDate !== "string" || snapshot.compatibilityDate.length > 32 || !Array.isArray(snapshot.compatibilityFlags) || snapshot.compatibilityFlags.length > 64 || !snapshot.compatibilityFlags.every((flag)=>typeof flag === "string" && flag.length <= 128)) {
        throw bindingError("VERSION_INVARIANT_VIOLATION");
    }
    return {
        compatibilityDate: snapshot.compatibilityDate,
        compatibilityFlags: [
            ...snapshot.compatibilityFlags
        ],
        allowExperimental: true,
        limits: snapshot.limits
    };
}
export async function snapshotWorkerCode(env, snapshot, scope) {
    const options = workerCodeOptions(snapshot);
    if (snapshot.mainModule?.endsWith(".py")) {
        if (snapshot.pythonPreparedSha256 === undefined) throw bindingError("PYTHON_PREPARED_ARTIFACT_MISSING");
        if (!/^[a-f0-9]{64}$/.test(snapshot.pythonPreparedSha256)) throw bindingError("VERSION_INVARIANT_VIOLATION");
        return {
            ...options,
            openComputePythonSnapshot: await resolvePythonSnapshot(env, snapshot, scope)
        };
    }
    return options;
}
export function pythonPreparationCode(snapshot) {
    if (snapshot.contentKind !== "worker" || snapshot.pythonPreparedSha256 !== undefined || !snapshot.mainModule?.endsWith(".py")) throw bindingError("VERSION_INVARIANT_VIOLATION");
    return workerCodeOptions(snapshot);
}
function policyInteger(env, name, maximum) {
    const value = Number(env[name]);
    if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
        throw bindingError("DO_INTERNAL_PROTOCOL_ERROR");
    }
    return value;
}
export function doPolicy(env) {
    return Object.freeze({
        maxObjectNameBytes: policyInteger(env, "DO_MAX_OBJECT_NAME_BYTES", 1024),
        maxFetchBodyBytes: policyInteger(env, "DO_MAX_FETCH_BODY_BYTES", 64 * 1024 * 1024),
        dispatchTimeoutMs: policyInteger(env, "DO_DISPATCH_TIMEOUT_MS", 5 * 60 * 1000),
        maxInFlightDispatches: policyInteger(env, "DO_MAX_IN_FLIGHT_DISPATCHES", 4096)
    });
}
export function currentStartupGeneration() {
    return startupGeneration ??= crypto.randomUUID();
}
export async function resolveSnapshot(env, envelope, scope) {
    const cacheKey = warmSnapshotKey(envelope, scope);
    if (cacheKey) {
        const cached = warmRuntimeSnapshots.get(cacheKey);
        if (cached) {
            const size = warmRuntimeSnapshotSizes.get(cacheKey) ?? 0;
            warmRuntimeSnapshots.delete(cacheKey);
            warmRuntimeSnapshotSizes.delete(cacheKey);
            warmRuntimeSnapshots.set(cacheKey, cached);
            warmRuntimeSnapshotSizes.set(cacheKey, size);
            return cached;
        }
    }
    const response = await env.RUNTIME_SOURCE.fetch(`http://runtime-source${SOURCE_PATH}`, {
        method: "POST",
        headers: {
            "content-type": "application/json"
        },
        body: JSON.stringify({
            key: envelope.loaderKey,
            expectedWorkerCodeSha256: envelope.expected,
            scope
        })
    });
    if (!response.ok) {
        const code = response.headers.get("x-open-compute-error-code") || "RUNTIME_INTERNAL";
        throw bindingError(code);
    }
    const snapshot = await response.json();
    assertSnapshot(snapshot);
    if (snapshot.loaderKey !== envelope.loaderKey || snapshot.workerCodeSha256 !== envelope.expected || scope === "runtime" && envelope.routeGeneration !== undefined && snapshot.routeGeneration !== envelope.routeGeneration) {
        throw bindingError("VERSION_INVARIANT_VIOLATION");
    }
    if (cacheKey) rememberWarmSnapshot(cacheKey, snapshot);
    return snapshot;
}
function snapshotBytes(snapshot) {
    let bytes = 256;
    for (const module of snapshot.modules ?? []){
        bytes += module.name.length + module.bytesBase64.length;
    }
    for (const binding of snapshot.moduleBindings ?? []){
        bytes += binding.name.length + binding.bytesBase64.length;
    }
    return bytes;
}
function dropWarmSnapshot(key) {
    const size = warmRuntimeSnapshotSizes.get(key);
    if (size === undefined) return;
    warmRuntimeSnapshotTotal -= size;
    warmRuntimeSnapshots.delete(key);
    warmRuntimeSnapshotSizes.delete(key);
}
function rememberWarmSnapshot(key, snapshot) {
    dropWarmSnapshot(key);
    const size = snapshotBytes(snapshot);
    warmRuntimeSnapshots.set(key, snapshot);
    warmRuntimeSnapshotSizes.set(key, size);
    warmRuntimeSnapshotTotal += size;
    while(warmRuntimeSnapshots.size > 1 && (warmRuntimeSnapshots.size > WARM_RUNTIME_SNAPSHOTS || warmRuntimeSnapshotTotal > WARM_RUNTIME_SNAPSHOT_BYTES)){
        const oldest = warmRuntimeSnapshots.keys().next().value;
        if (oldest === undefined || oldest === key) break;
        dropWarmSnapshot(oldest);
    }
}
export function assembleOnce(key, build) {
    const current = assembling.get(key);
    if (current) return current;
    const pending = build().finally(()=>{
        if (assembling.get(key) === pending) assembling.delete(key);
    });
    assembling.set(key, pending);
    return pending;
}
export function bindingError(code) {
    const error = Object.assign(new Error(code), {
        stableCode: code
    });
    error.stack = `Error: ${code}`;
    return error;
}
export function stableCode(error) {
    return error !== null && typeof error === "object" && !Array.isArray(error) && typeof Reflect.get(error, "stableCode") === "string" ? Reflect.get(error, "stableCode") : undefined;
}
export function isRecord(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
