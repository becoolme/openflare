// Test-only shim for the workerd builtin module `cloudflare:workers`.
// workerd instantiates bound entrypoint classes as `new Class(ctx, env)`
// where ctx.props carries the service-binding props; the shim mirrors that
// shape so vendored transports run unmodified under `node --test`.
export class WorkerEntrypoint {
  constructor(ctx, env) {
    this.ctx = ctx ?? {};
    this.env = env ?? {};
  }
}
export class RpcTarget {
  constructor(ctx, env) {
    this.ctx = ctx ?? {};
    this.env = env ?? {};
  }
}
export const env = {};
