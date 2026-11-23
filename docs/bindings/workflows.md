# Workflows

Cloudflare Workflows (`WorkflowEntrypoint` classes) running on a vendored
Cloudflare engine — a Durable Object inside the tenant's own workerd.

**Status: Partial**

## Wrangler configuration

From `examples/standalone-workflow/wrangler.toml`:

```toml
[[workflows]]
name = "order-flow"
binding = "WF"
class_name = "OrderFlow"
```

## Where it runs

- `env.WF` is a workerd `wrapped` binding that forwards to an internal
  `wf-engine-*` service's `WorkflowBinding` entrypoint
  (`vrt-config/src/capnp.rs`).
- The engine is the vendored Cloudflare Workflows engine (miniflare
  `5.20260916.0-alpha` lineage, MIT — `vrt-config/system-workers/
  workflows/`, bytes embedded into `vrt-config`), running as a Durable
  Object in the tenant's workerd under the same `doStorageDir` as user
  DO classes.
- `create()` / `createBatch()` confirm engine durability (bounded status
  poll) before resolving; a timeout throws a typed
  `instance.create_unconfirmed` error instead of handing out phantom ids
  (`vrt-config/system-workers/workflows/wrapped-binding.vrt.worker.js`).
- `of workflows` lists declared workflows; `of wfop` exposes engine
  operations; `of do-snapshot` / `of do-migrate` cover the engine's
  storage like any DO class.

## Backends

The engine's storage is `localDisk` SQLite (`vrt-host` `doStorageDir`),
same as Durable Objects — its `uniqueKey` is server-minted
(`HMAC(pepper, "vrt-wf:{tenant}:{definingWorker}:{wfName}")`), never
client input.

## Known differences from Cloudflare

- **Fixed per-Workflow concurrency limit.** A per-Workflow coordinator
  DO caps active instances at 4,500 by default; `create()` over the cap
  fails with `workflow.concurrency_limit` instead of queueing. The slot
  is released on completion/error/termination or explicit delete. This is
  a per-Workflow cap, not Cloudflare's account-wide quota; the limit is
  not yet configurable through the binding.
- **No per-step subrequest budget.** The signed config carries only
  `limits.steps`; workerd's worker-wide subrequest quota still applies.
- **Fixed terminal-instance retention.** Completed, terminated and errored
  instances are deleted seven days after reaching the terminal status
  (`binding.vrt.worker.js` alarm). Per-Workflow `default_retention` and
  per-instance `retention` are not wired yet, so the duration is not
  configurable through the binding.
- Deploy-time static gate: for same-worker classes, `class_name` must
  be a named export AND lexically `extends` a chain rooted at
  `WorkflowEntrypoint` (`vrt-local/src/wf_scan.rs`). Unresolvable shapes
  (`export *`, `export default`, computed bases, decorators, non-ESM
  exports) fail closed. See `examples/standalone-workflow-bad-class`.
- `script_name` may name an already-deployed worker of the same tenant
  (deploy order matters — the target must exist first); storage and
  engine ops belong to the defining worker, matching Cloudflare.
- Workflow `name` must match `^[A-Za-z0-9_-]{1,64}$` and be unique per
  worker; `limits.steps` is 1–100,000.
- A workflow-declaring worker never hibernates or scales to zero — the
  engine is a DO and its alarms cannot wake the host
  (`vrt-host/src/hibernate.rs`, `StructuralWorkflow`).
- `[[workflows]]` entries accept only `name`, `binding`, `class_name`,
  `script_name` and `limits.steps` — anything else fails closed.

## Unsupported keys and error shape

- Unknown `workflows[]` keys → `VRT_CONFIG_UNSUPPORTED_FIELD`
  (`workflows[0].<key>`), including `limits.<other>` than `steps`.
- `limits.steps` outside 1–100,000 → `VRT_CONFIG_INVALID`.
- A `class_name` that is not an exported `WorkflowEntrypoint` subclass
  fails the deploy with a typed worker error (not a wrangler schema
  error).

## Examples and tests

- `examples/standalone-workflow` — `[[workflows]]` on the vendored
  engine.
- `examples/standalone-workflow-bad-class` — the deploy gate rejecting a
  non-`WorkflowEntrypoint` class.
- `vrt-config/tests/workflows_emit.rs` — capnp emission tests.
- `vrt-config/tests/workflows_vendor.rs` — vendored-engine drift check.
- `vrt-local/tests/workflows.rs` — pack/declaration tests.
- `vrt-host/tests/workflows_smoke.rs` — end-to-end smoke test (skips
  without a real workerd).
