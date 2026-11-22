# Service bindings

Worker-to-worker bindings inside one tenant: `env.SVC.fetch()` and RPC
methods reach the named worker's default entrypoint, or a named
`WorkerEntrypoint` export when `entrypoint` is set. A JSON `props`
object, when declared, is surfaced to the target as `ctx.props`.

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
props = { region = "eu", flags = { beta = true } }   # ctx.props (optional)
```

Native `vrt.toml` equivalent:

```toml
[bindings.service]
SVC = "other-worker"
GREETER = { service = "other-worker", entrypoint = "Greeter" }
REGIONAL = { service = "other-worker", props = { region = "eu" } }
```

The plain-string form is the default entrypoint and keeps its original
wire bytes; the table form accepts exactly `service`, `entrypoint` and
`props` (unknown keys fail closed). `entrypoint` and `props` are both
optional inside the table.

## Where it runs

At pack time the `service` name is resolved to an already-deployed
worker of the same tenant (`vrt-local/src/worker_pkg.rs`) — a miss fails
closed. The emitted workerd binding is a service designator to
`worker-<workerId>` (`vrt-config/src/capnp.rs`), with
`entrypoint = "<name>"` added only when a named entrypoint is set, so
`fetch()`, `connect()` and RPC work against either the default export or
the named class. When `props` is declared it is emitted as
`props = (json = "<canonical compact JSON>")` on the same designator and
arrives as `ctx.props` in the target entrypoint. Cross-tenant service
bindings are impossible by construction (the target must exist in the
same tenant's worker list, enforced both at pack and in snapshot
validation).

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

## Props rules

`props` is optional. When present it must be:

- a JSON **object** (Cloudflare's documented `ctx.props` shape; scalars,
  arrays and `null` are rejected — an explicit `null` is not treated as
  absent);
- at most **8 KiB** (8192 bytes) as compact JSON;
- free of sibling keys inside the `[bindings.service]` table — the table
  itself accepts only `service`, `entrypoint` and `props`.

Nested objects, arrays, strings, numbers, booleans and `null` are legal
*inside* the object. The value is canonicalized (object keys sorted by
UTF-16 code unit order, no insignificant whitespace) before it is written
into the signed workerd designator, so identical declarations always
render identical bytes. The rules are enforced at wrangler normalization
(`services[].props`), at pack for both config paths
(`LocalError::Worker("service props")`), at snapshot validation
(`DESIRED_SERVICE_PROPS`), and again at render
(`CONFIG_CAPACITY_EXCEEDED`). Snapshots that predate the field stay
byte-identical because absent `props` is omitted from the wire.

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

## Unsupported keys and error shape

- `services[]` accepts only `binding`, `service`, `entrypoint` and
  `props` — anything else → `VRT_CONFIG_UNSUPPORTED_FIELD`
  (`services[0].<key>`).
- An `entrypoint` that breaks the rules above →
  `VRT_CONFIG_UNSUPPORTED_FIELD` (`bindings.<name>.entrypoint`), or
  `VRT_CONFIG_INVALID` for an empty string.
- A `props` that is not an object or exceeds 8 KiB →
  `VRT_CONFIG_INVALID` (`services[0].props`) at normalization, or
  `LocalError::Worker("service props")` for the native `vrt.toml`
  table/library path; snapshot validation reports
  `DESIRED_SERVICE_PROPS` and render uses
  `CONFIG_CAPACITY_EXCEEDED`.
- A `service` that does not resolve to a deployed worker →
  `LocalError::Worker("service target")` at deploy.

## Examples and tests

- `shared/vrt-project-config-vectors.json` — golden vectors covering
  `services` normalization (`service binding keeps entrypoint`).
- `vrt-local/src/project_cfg.rs` — normalization tests (golden vector
  harness `golden_vectors_match_typescript`), the standalone entrypoint
  tests (`service_entrypoint_into_pack_shape`,
  `service_entrypoint_rules_fail_closed`, `vrt_toml_service_table_shape`),
  and the props tests (`service_props_fail_closed`,
  `native_service_props_reach_pack_and_fail_closed`).
- `snapshot-protocol/src/compute_desired.rs` —
  `service_entrypoint_wire_shape`, `service_entrypoint_rules`,
  `service_props_wire_shape`, `service_props_rules`.
- `vrt-config/tests/render.rs` —
  `service_binding_named_entrypoint_renders`,
  `service_binding_reserved_entrypoint_is_refused`,
  `service_binding_props_render_only_when_declared`,
  `service_binding_invalid_props_is_refused_at_render`.
