# Secrets Store

`env.<binding>` is a Secrets Store binding — a read-only
`SecretsStoreSecret` stub whose `get()` resolves to the secret's
value, as on Cloudflare. `store_id` and `secret_name` travel as
service-binding props; the value itself never enters the rendered
config — it reaches the bundled secrets-store worker through an
`env:<secret_name>` `fromEnvironment` binding resolved from the
tenant's secret map.

**Status: Partial** — `[[secrets_store_secrets]]` normalizes,
renders, and serves `get()` end to end on the real workerd binary
(the bundled module is upstream code, no fork shim).

## Wrangler configuration

```toml
[[secrets_store_secrets]]
binding     = "SECRET"
store_id    = "default"      # optional — CF default "default"
secret_name = "api-key"
```

Native `vrt.toml` equivalent — `name -> {storeId?, secretName}`:

```toml
[bindings.secretsStore]
SECRET = { secretName = "api-key" }           # storeId defaults "default"
OTHER = { storeId = "prod", secretName = "other-key" }
```

`binding` and `secret_name` are required; `store_id` defaults to
`"default"`. Unknown keys fail closed
(`VRT_CONFIG_UNSUPPORTED_FIELD secrets_store_secrets[i].<key>`).

## Provisioning the value

Wrangler files never carry secret values. The binding resolves its
ref `secrets-store:{store_id}:{secret_name}` inside the owning
tenant's secret map — the same `params.secrets` namespace `[secrets]`
bindings use. Provision it out of band:

```sh
vrt secret put secrets-store:default:api-key
```

An unprovisioned ref does not render a half-wired binding: the worker
is excluded from the generation (`skipped_workers` +
`CONFIG_UNRESOLVED_REFERENCE`), its routes answer
`worker_unavailable`, siblings keep serving.

## Using it

```js
export default {
  async fetch(req, env) {
    const key = await env.SECRET.get(); // resolves to the value
    return new Response(`len=${key.length}`);
  },
};
```

## Behaviour notes

- `get()` is the only operation — a Worker can only READ a secret,
  as on Cloudflare.
- `get()` resolves to the secret's value; when the `env:<secret_name>`
  variable is unset at launch it throws
  `Error: Secret "<secret_name>" not found` — upstream's exact message.
- One `secret_name` has one value per worker: two bindings naming the
  same `secret_name` share the `env:<name>` slot; naming it with two
  different refs is ambiguous and the worker is excluded
  (`CONFIG_INVALID_IDENTIFIER`) — the same invariant upstream's
  `check_bindings` enforces at parse time.
- Values never reach the config text or its digest — they ride
  `fromEnvironment` + child-process env injection, the same channel
  `[secrets]` bindings use. Rotating a secret changes `env`, not the
  rendered bytes.

## Limits

- `store_id` / `secret_name` hold to the reference identifier charset
  `[A-Za-z0-9_\-.:]` (they become part of the derived ref); a
  violation fails validation (`bindings.storeId` /
  `bindings.secretName`).
- `env.<binding>` surface is `get()` only — `SecretsStoreSecret`
  exposes no `put`/`delete` on Cloudflare either.

## Where it runs

Each worker declaring secrets-store bindings gains a
`worker-<workerId>-secrets-store` service: the bundled
`/workerd/secrets-store.js` worker (compiled into the workerd binary)
serving `SecretsStoreSecret` entrypoints over service-binding RPC.
Its `env:<secret_name>` bindings are `fromEnvironment`; outbound is
deny-all — it cannot leave the tenant's generation.

## Backends

No adapter — the value is process-env material resolved by the
platform's secret map (`[secrets]`/`vrt secret put` out-of-band
provisioning), not a remote store. Account-level Secrets Store sync
is out of scope.

## Unsupported keys and error shape

Only `binding`, `store_id`, `secret_name` are accepted. Unknown keys:

```
[VRT_CONFIG_UNSUPPORTED_FIELD] secrets_store_secrets[0].remote is not supported by the VRT runtime
```

Missing `binding`/`secret_name` fails `VRT_CONFIG_INVALID`.

## Examples and tests

- `vrt-config/tests/secrets_store_emit.rs` — service-binding +
  bundled-worker emission, env wiring, value absence from config,
  same-name dedup, divergent-ref and missing-ref exclusion,
  `resolve_tenant_env` stub-path parity.
- `vrt-host/tests/bindings_workerd.rs::
  secrets_store_binding_serves_env_backed_value` — real-workerd smoke:
  `get()` returns the env-injected value; unset `env:` variable
  surfaces `Secret "<name>" not found` (`VRT_WORKERD_BIN` gated).

## Known differences from Cloudflare

- Cloudflare stores are account resources synced to the edge; VRT
  resolves the derived ref from the tenant's local secret map — the
  operator provisions `secrets-store:{store_id}:{secret_name}`.
- Upstream `wrangler` reads `WORKERD_SECRET_<NAME>` process envs;
  VRT derives a per-worker env name
  (`VRT_S_<workerId>__secrets-store:<secret_name>`) so two workers'
  same-named secrets never collide in the one generation process.
