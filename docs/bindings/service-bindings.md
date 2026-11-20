# Service bindings

Worker-to-worker bindings inside one tenant: `env.SVC.fetch()` and RPC
methods reach the named worker's default entrypoint.

**Status: Partial**

## Wrangler configuration

```toml
[[services]]
binding = "SVC"
service = "other-worker"   # worker name in the same tenant
```

Native `vrt.toml` equivalent:

```toml
[bindings]
service = { SVC = "other-worker" }
```

## Where it runs

At pack time the `service` name is resolved to an already-deployed
worker of the same tenant (`vrt-local/src/worker_pkg.rs`) — a miss fails
closed. The emitted workerd binding is a plain service designator to
`worker-<workerId>` (`vrt-config/src/capnp.rs`), so `fetch()` and
default-entrypoint RPCs work; cross-tenant service bindings are
impossible by construction (the target must exist in the same tenant's
worker list, enforced both at pack and in snapshot validation).

## Backends

None — a service binding is direct workerd-to-workerd service dispatch
inside the tenant's process.

## Known differences from Cloudflare

- **No named entrypoints.** `services[].entrypoint` parses into the
  normalized config (the shared golden vectors keep it) but the
  standalone `vrt.toml` wire shape has no entrypoint field, so deploy
  fails with `VRT_CONFIG_UNSUPPORTED_FIELD`
  `bindings.<name>.entrypoint is not supported by the VRT runtime
  (standalone service bindings have no entrypoint)`.
- The target must be deployed before the referencing worker (deploy
  order matters); there is no Cloudflare-style account-wide registry —
  only the tenant's own workers.
- `env.<binding>` covers `fetch`, `connect` and RPC to the **default**
  entrypoint only.

## Unsupported keys and error shape

- `services[]` accepts only `binding`, `service`, `entrypoint` —
  anything else → `VRT_CONFIG_UNSUPPORTED_FIELD`
  (`services[0].<key>`).
- `entrypoint` → `VRT_CONFIG_UNSUPPORTED_FIELD` at pack (see above).
- A `service` that does not resolve to a deployed worker →
  `LocalError::Worker("service target")` at deploy.

## Examples and tests

- `shared/vrt-project-config-vectors.json` — golden vectors covering
  `services` normalization (`service binding keeps entrypoint`).
- `vrt-local/src/project_cfg.rs` — normalization tests (golden vector
  harness `golden_vectors_match_typescript`).
