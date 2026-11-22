# Unsafe bindings (`unsafe_eval`)

`env.<name>` is workerd's native `UnsafeEval` capability object. The
worker that holds it can compile and run code that is not part of its
uploaded module graph:

- `env.<name>.eval(script, name?)` — compile and run a script
- `env.<name>.newFunction(script, name?, ...argNames)` /
  `newAsyncFunction(...)` — compile a function outside the bundle
- `env.<name>.newWasmModule(bytes)` — compile WebAssembly bytes

This is an explicit API surface, not a mode switch: the globals
`eval()` / `new Function()` / `new WebAssembly.Module()` stay blocked
inside request handlers (workerd only allows those at module scope
under `allow_eval_during_startup`, which this binding does not change).

**Status: Partial**

## Wrangler configuration

```toml
[[unsafe.bindings]]
name = "EVAL"
type = "unsafe_eval"
```

`type = "unsafe_eval"` is the only accepted binding type — every other
value fails closed (`VRT_CONFIG_UNSUPPORTED_FIELD`,
`unsafe.bindings[0].type`), because every other binding kind either
has a first-class wrangler key or has no runtime support at all. Only
`name` and `type` may appear in an entry; any other per-entry key is
rejected the same way. `[unsafe]` accepts only `bindings` — the raw
injection keys (`capnp`, `metadata`, …) are refused.

## Where it runs

The binding renders workerd's `unsafeEval` capnp group — a pure
capability flag on the worker, no service or socket is wired. With it,
`env.<name>.eval("1+1")`, `env.<name>.newFunction("return 1")()` and
`env.<name>.newWasmModule(bytes)` all work; without the binding the
value is `undefined`, so every use throws `TypeError`.

## Backends

Native workerd capability — there is no provider resource behind the
binding and nothing to migrate or persist.

## Known differences from Cloudflare

- **Admission is per cell release, not per account.** Cloudflare ships
  unsafe bindings behind account allow-listing. Here the `unsafeEval`
  ACK bit is reported unconditionally by `cell-controller`
  (`cell-controller/src/bin/cell-controller.rs`), so any tenant on a
  cell running this release may declare the binding — plain-parity
  intent, stated explicitly. The cell ACK mechanism is where a future
  per-tenant opt-in would hang.
- **Bundle analysis caveat:** a worker holding `unsafeEval` defeats
  static inspection of its module graph — code can be synthesized at
  runtime through `env.<name>.eval`/`newFunction`/`newWasmModule`
  rather than only shipped in the uploaded bundle. Treat the binding as opting the
  worker out of code-reviewable deployment artifacts.
- The remaining `unsafe.*` escape hatches Cloudflare exposes
  (`capnp` schema injection, raw `metadata`) stay refused — they would
  bypass desired-state validation entirely.

## Unsupported keys and error shape

- `[[unsafe.bindings]]` accepts only `name` and `type`; anything else →
  `VRT_CONFIG_UNSUPPORTED_FIELD` (`unsafe.bindings[0].<key>`).
- `type` other than `"unsafe_eval"` → `VRT_CONFIG_UNSUPPORTED_FIELD`
  (`unsafe.bindings[0].type`).
- Other `[unsafe]` keys (`capnp`, `metadata`, …) →
  `VRT_CONFIG_UNSUPPORTED_FIELD` (`unsafe.<key>`).
- Missing/non-string `name` or `type` → `VRT_CONFIG_INVALID`
  (`unsafe.bindings[0].name` / `.type`).
- A binding name colliding with another binding on the same worker →
  `DESIRED_DUPLICATE` (`bindingName`) at snapshot validation.

## Examples and tests

- `vrt-local/src/project_cfg.rs` — `unsafe_eval_bindings_fail_closed`
  (unknown type, non-table, missing keys, unknown per-entry key).
- `shared/vrt-project-config-vectors.json` — `unsafe_eval` accept
  vector plus reject vectors for a non-eval `type`, a raw `[unsafe]`
  injection key, and an unknown per-entry key (Rust/TS parity).
- `vrt-config/tests/render.rs` — capnp `unsafeEval` group emission.
