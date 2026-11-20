# Vars and secrets

`vars` from wrangler become workerd `text` bindings; secrets are
`secretRef` handles resolved host-side and injected as `fromEnvironment`
bindings — values never sit in the bundle.

**Status: Partial**

## Wrangler configuration

From `examples/standalone-wrangler/wrangler.jsonc`:

```jsonc
{
  "vars": { "GREETING": "hello" },
  "compatibility_date": "2024-09-01"
}
```

Secret values never appear in any config file. In a `vrt.toml` project
the `[secrets]` table maps binding name → secretRef, and the value is
stored separately (`examples/standalone-hello/vrt.toml`):

```toml
[vars]
LOG_LEVEL = "info"

[secrets]
GREETING_SECRET = "greeting-secret"   # secretRef; value via:
# printf 'my-secret' | vrt secret put --root <root> greeting-secret
```

## Where it runs

- `vars` → `BindingDesired::Var` in the signed snapshot, resolved to
  workerd `text` bindings by `vrt-config` (`vrt-config/src/capnp.rs`).
  Standalone `var_ref` is `var:{name}:{vhash8}:{binding}` — the value is
  part of the version identity (`vrt-local/src/worker_pkg.rs`).
- `[secrets]` → `BindingDesired::Secret`; the capnp emitter renders a
  `fromEnvironment` binding named
  `VRT_S_<enc(workerId)>__<enc(bindingName)>` and hands `vrt-host` a
  sealed env map — the value is injected into the workerd process
  environment at launch, never into the config text or its digest
  (`vrt-config/src/capnp.rs`).

## Backends

Secret values live in `secrets/secrets.json` (mode `0600`,
`vrt-local/src/layout.rs`), managed by `vrt secret put|ls|rm`. `put`
reads the value from stdin only — never argv.

## Known differences from Cloudflare

- `vars` values arrive as **strings**. Cloudflare injects non-string
  `vars` (numbers, booleans, objects) as typed JSON values; OpenFlare
  JSON-encodes them into text bindings, so `env.NUM` is `"42"` not
  `42` — read with `JSON.parse` or coercion
  (`vrt-local/src/project_cfg.rs` standalone var conversion).
- There is no `wrangler secret` CLI and no secret value in the wrangler
  file — only `secretRef` names plus `vrt secret put`.
- `keep_vars` is accepted at top level but ignored (deploys always carry
  their own `vars`).
- `.dev.vars` is not read.

## Unsupported keys and error shape

- `secrets_store_secrets` (Secrets Store bindings) →
  `VRT_CONFIG_UNSUPPORTED_FIELD`, as does any other per-entry key there.
- Values are redacted from TOML error messages — a secret-looking
  `vars` value cannot leak into an error.

## Examples and tests

- `examples/standalone-hello` — one `vars` entry + one `secretRef`.
- `examples/standalone-wrangler` — `vars` in `wrangler.jsonc`.
- `vrt-local/src/project_cfg.rs` — wrangler `vars` normalization.
- `vrt-local/src/worker_pkg.rs` — `var:`/`secret:` ref derivation.
- `vrt-local/src/bin/vrt.rs` — `vrt secret put|ls|rm` implementation.
