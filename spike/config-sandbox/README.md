# `cloudflare.config.*` sandbox spike

Proof of concept for evaluating `cloudflare.config.ts` inside a workerd
dynamic worker instead of a Node child process. **Not wired into
`of deploy`** — see [docs/config-sandbox-spike.md](../../docs/config-sandbox-spike.md)
for the results, timings and conclusions.

```
pnpm install                                     # @cloudflare/config for the fixture
cargo build --manifest-path of-bundler/Cargo.toml

# upstream workerd (darwin/linux) — same upstream commit as the pin
node spike/config-sandbox/run.mjs --workerd /path/to/workerd --case parity

# pinned fork (linux) under Docker
node spike/config-sandbox/run.mjs \
  --docker-image debian:12-slim \
  --workerd /path/to/linux-workerd \
  --case parity

# all cases
node spike/config-sandbox/verify.mjs --workerd /path/to/workerd
```

Cases:

| case | what it asserts |
| --- | --- |
| `parity` | bundle -> dynamic worker -> JSON -> trusted `convertToWranglerConfig` equals `scripts/load-cloudflare-config.mjs` |
| `fs-import` | a config reading a host path is blocked (workerd's virtual FS has no host files) |
| `network` | a config fetching an address is denied by the dynamic worker's outbound policy |
| `loop` | `while (true)` is bounded by the caller's wall-clock deadline |

The runner needs a `@cloudflare/config` install (repo devDependency) and a
workerd binary. It never downloads anything.
