# `cloudflare.config.*` sandbox verification

The sandbox itself lives in `scripts/load-cloudflare-config-worker.mjs`
and is wired into `of deploy` / `of check` as `--config-sandbox=worker`
(default: `node`). This directory only keeps the end-to-end verification
harness; the design, semantics and known gaps are in
[docs/config-sandbox-spike.md](../../docs/config-sandbox-spike.md).

```
# prerequisites (same pins as every other deploy path)
cargo build --manifest-path of-bundler/Cargo.toml
pnpm install

# full matrix: parity, nested factories, env injection, cf-worker,
# dependencies, host FS, network, oversized payload, invalid config,
# while(true) timeout, missing workerd, 10x repeat + concurrency,
# impossible-allocation fail-closed, real-example parity
node spike/config-sandbox/verify.mjs --workerd /path/to/workerd

# Rust integration matrix (self-skips without VRT_WORKERD_BIN; CI sets
# VRT_REQUIRE_WORKERD=1 so the real-workerd job fails instead of skipping)
VRT_WORKERD_BIN=/path/to/workerd \
  cargo test -p vrt-local --test config_sandbox
```

`verify.mjs` drives the two production helpers directly
(`scripts/load-cloudflare-config.mjs` as the baseline and
`scripts/load-cloudflare-config-worker.mjs` for the sandbox); it does not
re-implement the chain. It exits `2` when a prerequisite is missing and
`1` when an assertion fails. CI runs it in the `real-workerd` job.
