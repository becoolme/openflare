# Service bindings

Worker-to-worker bindings inside one tenant: `env.SVC.fetch()` and RPC
methods reach the named worker's default entrypoint, or a named
`WorkerEntrypoint` export when `entrypoint` is set.

**Status: Partial**

## Wrangler configuration

```toml
[[services]]
binding = "SVC"
service = "other-worker"   # worker name in the same tenant

[[services]]
binding = "GREETER"
service = "other-worker"
entrypoint = "Greeter"     # named WorkerEntrypoint export (optional)
props = { tier = "gold" }  # static props for ctx.props (optional)
```

Native `vrt.toml` equivalent:

```toml
[bindings.service]
SVC = "other-worker"
GREETER = { service = "other-worker", entrypoint = "Greeter", props = '{"tier":"gold"}' }
```

The plain-string form is the default entrypoint and keeps its original
wire bytes; the table form accepts `service` plus optional `entrypoint`
and `props` (unknown keys fail closed). `props` is a JSON object —
every invocation through the binding, RPC and `fetch()` alike, sees it
as `ctx.props` in the target, matching Cloudflare.

## Where it runs

At pack time the `service` name is resolved to an already-deployed
worker of the same tenant (`vrt-local/src/worker_pkg.rs`) — a miss fails
closed. The emitted workerd binding is a service designator to
`worker-<workerId>` (`vrt-config/src/capnp.rs`), with
`entrypoint = "<name>"` added only when a named entrypoint is set, so
`fetch()`, `connect()` and RPC work against either the default export or
the named class. Cross-tenant service bindings are impossible by
construction (the target must exist in the same tenant's worker list,
enforced both at pack and in snapshot validation).

Named entrypoints reach workerd because the platform `waitUntil`
wrapper re-exports the user module (`export * from <main>`).

## Entrypoint rules

`entrypoint` is optional; when present it must be:

- a JS identifier of at most 64 characters;
- not `default` (omit the field for the default export);
- not starting with `Vrt` / `vrt` — that prefix is reserved for
  platform-injected entrypoints such as `VrtWaituntilProbe`.

The rules are enforced three times: at pack
(`VRT_CONFIG_UNSUPPORTED_FIELD`, field `bindings.<name>.entrypoint`), at
snapshot validation (`DESIRED_INVALID_IDENTIFIER`,
`bindings.entrypoint`), and again at render
(`CONFIG_SERVICE_TARGET_SCOPE`; the offending caller worker is excluded
from the generation while its siblings keep serving). Older hosts that
predate the field reject it via `deny_unknown_fields` — fail closed, not
silently routed to the default export.

## Backends

None — a service binding is direct workerd-to-workerd service dispatch
inside the tenant's process.

## Known differences from Cloudflare

- **Missing named export fails at publish, not at call time.** workerd
  validates the entrypoint when the generation starts; if the target
  does not export the class, the generation fails to start and the
  publish rolls back. Cloudflare instead returns an error when the
  binding is first called.
- **`ctx.waitUntil()` inside a named `WorkerEntrypoint` class is not
  tracked** by the platform drain probe (only the default export is
  wrapped); pending promises there may be cut short on hibernation or
  rollout.
- The target must be deployed before the referencing worker (deploy
  order matters); there is no Cloudflare-style account-wide registry —
  only the tenant's own workers.
- `props` accepts a JSON object only (CF types it
  `Record<string, unknown>`) up to 16 KiB serialized; Cloudflare
  additionally allows per-call props on JS RPC arguments, which is a
  runtime API surface not modelled here.

## Unsupported keys and error shape

- `services[]` accepts only `binding`, `service`, `entrypoint`, `props`
  — anything else → `VRT_CONFIG_UNSUPPORTED_FIELD`
  (`services[0].<key>`).
- `props` that is not a JSON object, or serializes past 16 KiB →
  `VRT_CONFIG_UNSUPPORTED_FIELD` (`services[0].props` /
  `bindings.<name>.props`), `DESIRED_INVALID_IDENTIFIER`
  (`bindings.serviceProps`) or `DESIRED_CAPACITY`
  (`servicePropsBytes`) at the snapshot boundary.
- An `entrypoint` that breaks the rules above →
  `VRT_CONFIG_UNSUPPORTED_FIELD` (`bindings.<name>.entrypoint`), or
  `VRT_CONFIG_INVALID` for an empty string.
- A `service` that does not resolve to a deployed worker →
  `LocalError::Worker("service target")` at deploy.

## Examples and tests

- `shared/vrt-project-config-vectors.json` — golden vectors covering
  `services` normalization (`service binding keeps entrypoint`).
- `vrt-local/src/project_cfg.rs` — normalization tests (golden vector
  harness `golden_vectors_match_typescript`) and the standalone
  entrypoint tests (`service_entrypoint_into_pack_shape`,
  `service_entrypoint_rules_fail_closed`, `vrt_toml_service_table_shape`,
  `service_props_into_pack_shape`, `service_props_must_be_object`).
- `snapshot-protocol/src/compute_desired.rs` —
  `service_entrypoint_wire_shape`, `service_entrypoint_rules`, plus the
  `service-props-*` validation vectors.
- `vrt-config/tests/render.rs` —
  `service_binding_named_entrypoint_renders`,
  `service_binding_reserved_entrypoint_is_refused`,
  `service_binding_props_renders`,
  `service_binding_props_without_entrypoint_renders`.
