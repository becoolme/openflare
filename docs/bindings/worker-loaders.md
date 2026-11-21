# Worker loaders (Dynamic Workers)

`env.LOADER` is a native workerd `WorkerLoader`: the worker supplies
arbitrary Worker code at runtime through `get(name, () => code)` and
`load(code)`, calls its entrypoints, and pulls Durable Object classes
out of it — the Dynamic Workers surface of Workers for Platforms.

**Status: Partial**

## Wrangler configuration

```toml
[[worker_loaders]]
binding = "LOADER"
```

Native `vrt.toml` equivalent:

```toml
[bindings]
workerLoaders = ["LOADER"]
```

`binding` is the only accepted key — anything else fails closed
(`VRT_CONFIG_UNSUPPORTED_FIELD`, `worker_loaders[0].<key>`). The loader
points at no platform resource; all code comes from the worker itself.

## Using it

```js
export default {
  async fetch(req, env) {
    const code = await (await fetch(new URL(req.url).pathname)).text();
    // Named workers are cached per loader namespace; re-`get` returns
    // the existing stub until it is released.
    const stub = env.LOADER.get("tenant-plugin", async () => ({
      compatibilityDate: "2026-01-01",
      mainModule: "main.js",
      modules: { "main.js": `export default { fetch() { return new Response("hi"); } }` },
    }));
    return stub.getEntrypoint().fetch(req);
  },
};
```

The full upstream surface works: `WorkerStub.getEntrypoint(name?,
{limits})`, `getDurableObjectClass(name?, {limits})`, `ctx.facets`,
named-cache reuse, and `WorkerCode.limits` — all documented semantics
of the `worker_loader` API, including the fork additions below.

## Where it runs

The binding renders workerd's own `workerLoader` group
(`vrt-config/src/capnp.rs`) and the tenant process gets
`--experimental` (assemble adds it when any worker declares a loader).
Dynamic workers run **inside the same tenant workerd process** — the
same isolate boundary model as service bindings, never a separate
tenant. The upstream runtime compiles, links and caches them; named
stubs live in the loader's namespace keyed by `id`.

## Backends

None. A worker loader is a runtime-only binding: no platform
component, socket, service or storage is wired, and `worker_loaders[]`
accepts no resource-identifying keys besides `binding`. The loaded code
and its `WorkerCode.env` are supplied by the tenant worker itself, and
the only platform-side controls are the per-loader live-worker quota
(`WORKER_LOADER_MAX_WORKERS`) and the tenant workerd's own limits.

## Limits and quota

Two independent bounds apply, matching Cloudflare's documented
semantics plus a platform guard:

- **`WorkerCode.limits` / `{limits}` options** — `cpuMs`,
  `subRequests`, `memory`-family fields are honored with the
  documented *lower-of-two* rule: the effective limit is the lower of
  the caller-supplied value and the loading worker's own `limits`
  group. A dynamic worker can never exceed its loader's ceiling.
- **Live-worker quota** — each loader may hold at most **32** live
  workers (a platform constant, `WORKER_LOADER_MAX_WORKERS` in
  `vrt-config/src/capnp.rs`, emitted as `maxWorkers` to the pinned
  fork binary). Named cache entries and one-shot `load()` workers
  count alike; `get()`/`load()` past the bound throws until a held
  stub/entrypoint releases a worker. Cloudflare exposes no tenant knob
  for this, so there is none here either — the constant bounds a
  runaway `get()` loop's compiled-isolate memory inside the tenant
  process.

## Isolation and revocation

- A loader namespace is private to the declaring tenant workerd —
  dynamic workers can only reach the code the loader's own `get`/`load`
  callbacks return and the bindings forwarded in `WorkerCode.env`.
- Dynamic workers cannot themselves hold a `workerLoader` binding
  (nested loading fails closed inside workerd).
- Deploying a new generation **replaces the tenant workerd process**:
  every cached dynamic worker, stub and entrypoint handle is revoked
  with the process. There is no cross-generation leak to drain.
- A worker holding a loader is excluded from the warm-generation
  routing optimizations that assume no Durable Object reach —
  `getDurableObjectClass()` is an actor-namespace path the static
  binding graph cannot see (`do_tainted_workers`, `supervisor.rs`).

## Known differences from Cloudflare

- **Quota**: Cloudflare does not document a per-loader bound; the
  platform constant 32 is our guardrail, not a documented CF limit.
- **No `dispatch_namespaces`**: the older Workers-for-Platforms API
  (`env.NS.get(name)` over platform-deployed workers) is still
  rejected — `worker_loaders` is the native path.
- **No in-band revocation API**: open-compute's fork exposes
  `revokeNamespacePrefix`; we rely on the per-generation process model
  instead (new deploy = new process = all namespaces revoked).
- Requires `--experimental` on the tenant workerd (upstream gates
  `workerLoader` behind it); the platform adds the flag automatically.

## Unsupported keys and error shape

- `worker_loaders[]` accepts only `binding` — anything else →
  `VRT_CONFIG_UNSUPPORTED_FIELD` (`worker_loaders[0].<key>`).
- Missing/non-string `binding` → `VRT_CONFIG_INVALID`
  (`worker_loaders[0].binding`).
- A binding name colliding with another binding on the same worker →
  `DESIRED_DUPLICATE` (`bindingName`) at snapshot validation.
- `get()`/`load()` past the live-worker quota throws in the calling
  worker. The bound is enforced in the loader's kj layer, so the JS
  caller sees workerd's generic `internal error; reference = <id>`
  rather than a descriptive message (observed on the pinned fork
  binary); a held stub/entrypoint must be released before loading
  again.

## Examples and tests

- `vrt-local/src/project_cfg.rs` — `worker_loaders_into_pack_shape`,
  `worker_loaders_fail_closed` (unknown key / missing binding /
  non-string / wrong shape).
- `vrt-config/tests/render.rs` —
  `worker_loader_binding_renders_native_loader` (capnp group +
  quota emission, no service/socket wired).
- `vrt-host/src/supervisor.rs` — `taint_marks_worker_loader_holders`
  (DO-reachability taint for early-flip exclusion).
- workerd fork (`becoolme/workerd`, `feat/loader-quotas`) —
  `worker-loader-quota-test.wd-test` exercises the `maxWorkers`
  accounting in the runtime itself.
- `vrt-host/tests/workerd_entry_smoke.rs` —
  `real_workerd_worker_loader` (real binary, `VRT_WORKERD_BIN`-gated):
  `env.LOADER.get()`/`load()` serve, the named cache runs the code
  callback once, and a held-stub loop hits the `maxWorkers` bound.
