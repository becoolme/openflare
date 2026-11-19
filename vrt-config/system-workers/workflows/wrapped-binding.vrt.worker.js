// src/workers/workflows/wrapped-binding.worker.ts — VRT fork (W3-r4).
// Vendored `binding.worker.js` stays byte-identical; this wrapper is
// our layer (not checksum-pinned by SHA256SUMS). Vendored create()
// resolves {id} while stub.init() commits INSTANCE_METADATA
// asynchronously via ctx.waitUntil — a workerd death inside that
// window hands the caller a permanently un-queryable id (phantom).
//
// Durability signal — two facts decide it:
//  1. A BARE `binding.get()` success proves NOTHING: `init` assigns the
//     engine's in-memory `accountId` (binding.worker.js:4536) BEFORE
//     `put(INSTANCE_METADATA)` (:4558), and `getStatus()` short-circuits
//     on that memory path — `get()` can answer while the record is
//     still only in DO memory, uncommitted.
//  2. A NON-"queued" status IS durable: `ENGINE_STATUS_KEY` is written
//     only via `setStatus` (:4567+) strictly AFTER the metadata put,
//     and DO storage ops commit per-call through the output gate —
//     reading it back means the transaction committed (verified live
//     by the create→SIGKILL→restart smoke).
// So the rule is: poll `get()`+`status()` until the status is NOT
// "queued". A legitimately committed "queued" is the only
// false-negative; a bounded retry converges it.
async function confirmInstance(binding, id, budgetMs) {
  const deadline = Date.now() + budgetMs;
  for (;;) {
    let confirmed = false;
    try {
      using inst = await binding.get(id);
      const st = await inst.status();
      confirmed = st && st.status && st.status !== "queued";
    } catch (e) {
      const msg = String(e && e.message ? e.message : e);
      if (!msg.includes("instance.not_found")) throw e;
    }
    if (confirmed) return;
    if (Date.now() >= deadline) {
      const err = new Error(
        "instance.create_unconfirmed: instance not committed to engine storage within " +
          budgetMs +
          "ms",
      );
      err.name = "WorkflowCreateUnconfirmed";
      err.code = "instance.create_unconfirmed";
      throw err;
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}
var WorkflowImpl = class {
  constructor(binding, createConfirmMs) {
    this.binding = binding;
    this.createConfirmMs = createConfirmMs;
  }
  binding;
  createConfirmMs;
  async get(id) {
    let instanceHandle = new InstanceImpl(id, this.binding);
    return await instanceHandle.status(), instanceHandle;
  }
  async create(options) {
    using result = await this.binding.create(options);
    await confirmInstance(this.binding, result.id, this.createConfirmMs);
    return new InstanceImpl(result.id, this.binding);
  }
  async createBatch(options) {
    const res = await this.binding.createBatch(options);
    await Promise.all(
      res.map((r) => confirmInstance(this.binding, r.id, this.createConfirmMs)),
    );
    return res.map((r) => new InstanceImpl(r.id, this.binding));
  }
  async deleteBatch(instanceIds) {
    return this.binding.deleteBatch({ instances: instanceIds });
  }
  async unsafeGetBindingName() {
    return this.binding.unsafeGetBindingName();
  }
  async unsafeStartIntrospection() {
    return this.binding.unsafeStartIntrospection();
  }
  async unsafeStopIntrospection(sessionId) {
    return this.binding.unsafeStopIntrospection(sessionId);
  }
  async unsafeSetIntrospectionOperations(sessionId, operations) {
    return this.binding.unsafeSetIntrospectionOperations(sessionId, operations);
  }
  async unsafeGetIntrospectionInstances(sessionId) {
    return this.binding.unsafeGetIntrospectionInstances(sessionId);
  }
  async unsafeAbort(instanceId, reason) {
    return this.binding.unsafeAbort(instanceId, reason);
  }
  async unsafeGetInstanceModifier(instanceId) {
    return this.binding.unsafeGetInstanceModifier(instanceId);
  }
  async unsafeWaitForStepResult(instanceId, name, index) {
    return this.binding.unsafeWaitForStepResult(instanceId, name, index);
  }
  async unsafeWaitForStatus(instanceId, status) {
    return await this.binding.unsafeWaitForStatus(instanceId, status);
  }
  async unsafeGetOutputOrError(instanceId, isOutput) {
    return this.binding.unsafeGetOutputOrError(instanceId, isOutput);
  }
}, InstanceImpl = class {
  constructor(id, binding) {
    this.id = id;
    this.binding = binding;
  }
  id;
  binding;
  async getInstance() {
    return await this.binding.get(this.id);
  }
  async pause() {
    using instance = await this.getInstance();
    await instance.pause();
  }
  async resume() {
    using instance = await this.getInstance();
    await instance.resume();
  }
  async terminate(options) {
    using instance = await this.getInstance();
    await instance.terminate(options);
  }
  async restart(options) {
    using instance = await this.getInstance();
    await instance.restart(options);
  }
  async delete() {
    await this.binding.deleteInstance(this.id);
  }
  async status() {
    using instance = await this.getInstance(), res = await instance.status();
    return structuredClone(res);
  }
  async subscribe(options) {
    using instance = await this.getInstance();
    return await instance.subscribe(options);
  }
  async sendEvent(args) {
    using instance = await this.getInstance();
    return await instance.sendEvent(args);
  }
};
function makeBinding(env) {
  return new WorkflowImpl(
    env.binding,
    Number(env.createConfirmMs) || 10000,
  );
}
var wrapped_binding_vrt_worker_default = makeBinding;
export {
  wrapped_binding_vrt_worker_default as default,
  makeBinding
};
