# Per-worker quotas (`limits`)

Every worker runs with a declared quota set — CPU budget, isolate memory,
outbound subrequests and outbound connections. The values travel in the
signed `compute.desired` snapshot (`quotas { cpuMs, memoryMb, subrequests,
outboundConnections }`). This page documents how a declaration becomes an
enforced limit, and what "not enforced" looks like.

## Declaring limits

### Wrangler (`wrangler.toml` / `wrangler.jsonc`)

Cloudflare's `limits` block maps onto the snapshot quotas:

```toml
[limits]
cpu_ms = 200
subrequests = 150
```

| wrangler key | quota | range |
| --- | --- | --- |
| `cpu_ms` | `quotas.cpuMs` | 1..300000 ms (5 min, the Cloudflare cap) |
| `subrequests` | `quotas.subrequests` | 0..10000 (platform cap; Cloudflare paid allows more) |

`limits` is a partial override: an absent dimension keeps the platform
default. Unknown keys (`limits.memory_mb`, ...) are rejected with
`VRT_CONFIG_UNSUPPORTED_FIELD`, out-of-range values with
`VRT_CONFIG_INVALID` naming the exact path (`limits.cpu_ms`). Nothing is
clamped.

### `vrt.toml` (standalone)

```toml
[quotas]
cpuMs = 200
memoryMb = 192
subrequests = 150
outboundConnections = 16
```

Every field is optional; absent dimensions take the platform default
(50 ms CPU / 128 MiB / 50 subrequests / 16 outbound connections). Values
outside the protocol bounds fail the deploy with the field name
(`quotas.cpuMs`, ...).

### Platform API

The deploy request accepts a `quotas` object with the same four dimensions.
Omitting it means "no declaration" (platform defaults). On a cell that does
not report the `quotas` capability, declaring a non-default quota is a
`409 VRT_CAPABILITY_UNSUPPORTED`.

## How a declaration is rendered

For each worker `vrt-config` emits a workerd `limits` group — a VRT fork
extension (`Worker.limits` in `workerd.capnp`):

```capnp
worker = (
  compatibilityDate = "2026-11-01",
  limits = (cpuMs = 200, subRequests = 150),
  ...
)
```

Rules:

- **Only declared dimensions are emitted.** A dimension equal to the
  platform default is "not declared" and is omitted — an upgraded cell
  renders byte-identically for a worker that never set limits, so stock
  workerd behaviour (no limit) is preserved.
- **0 is never emitted.** `0` means "no declared budget" (`subrequests = 0`);
  workerd reads an absent dimension as unlimited, so the config never claims
  a limit of zero.
- **`outboundConnections` is not rendered.** The pinned fork has no
  connection-counter hook yet. A positive non-default value is refused
  (`CONFIG_QUOTA_UNSUPPORTED`) instead of being silently ignored; `0` and the
  platform default (16) are accepted as "not declared".
- Only `cpuMs`, `memoryMb` and `subRequests` reach workerd. `memoryMb` bounds
  the V8 heap (not native/SQLite memory) and startup evaluation is not
  CPU-limited.

## Cell capability (`quotas`)

The capability bit is reported in every desired-state ACK
(`capabilities.quotas`), like `workerLoader`. Semantics:

| cell state | declared non-default quota | undeclared (defaults) |
| --- | --- | --- |
| `quotas: true` (pinned fork) | rendered and enforced | no `limits`, platform default not enforced — surfaced as `quotasEnforced: false` |
| `quotas` absent/false (stock workerd / pre-fork cell) | **409 `VRT_CAPABILITY_UNSUPPORTED`** at deploy (control plane); if it still reaches the host, assembly fails with `CONFIG_QUOTA_UNSUPPORTED` and the previous generation keeps serving | allowed; the deploy response carries a `WRANGLER_FIELD_UNENFORCED` warning (control-plane side) |

The bit is opt-in (`quotas = true` in the cell-controller config) and is
**omitted from the ACK while false**, so a control plane whose strict schema
does not know the key yet keeps accepting acks. Enable it per cell only after
the control-plane schema ships; until then a declared non-default quota is
refused with 409 (fail closed). The vrt-host side (`[render] quotas`) defaults
ON because the pinned workerd
([`deploy/docker/workerd.lock`](../deploy/docker/workerd.lock)) carries the
patch; set it to `false` only for a stock-workerd host.

Fail-closed layers, in order:

1. control plane: capability missing + declared non-default quota → 409;
2. `vrt-config::ensure_quotas_enforceable` (host assemble / standalone
   preflight) → `CONFIG_QUOTA_UNSUPPORTED`, no new generation is written;
3. `workerd test` config dry-run: an unknown `limits` field fails the load,
   the previous generation keeps serving.

## Diagnostics

`of status` reports, per worker:

```json
"workers": [
  {
    "name": "api",
    "workerId": "w_...",
    "quotas": { "cpuMs": 200, "memoryMb": 128, "subrequests": 150, "outboundConnections": 16 },
    "quotasEnforced": true
  }
]
```

- `quotas` is the declared set (platform defaults when nothing was declared).
- `quotasEnforced` is `true` only when the root's host renders `limits`
  (`render.quotas`) **and** the worker declared at least one enforceable
  dimension. Platform defaults are never rendered, so a worker that declared
  nothing reads `false` — the diagnostic never claims an enforcement that
  did not happen.

`vrt-host` writes the pod-level flag to `<workDir>/host-status.json` as
`quotasEnforced`, which `of status` merges under `host.status`.

## Testing

| layer | test |
| --- | --- |
| render | `cargo test -p vrt-config` — golden (`tests/golden/tenant.capnp`), declared-dimension emit, 0/default omission, out-of-bounds and `outboundConnections` rejection, capability gate |
| host | `cargo test -p vrt-host --lib`; `tests/assemble.rs::quotas_capability_off_fails_closed` |
| standalone | `cargo test -p vrt-local` — wrangler `limits` normalization, partial `[quotas]`, `of status` diagnostics |
| cell | `cargo test -p cell-controller` — ACK `quotas` bit (omit while false) |

Real-workerd enforcement (CPU 1102 / subrequest / isolate OOM) lives in the
fork's own test suite and the design doc
[`doc/platform/workers-runtime-quotas.md`](https://github.com/becoolme/vibeploy/blob/main/doc/platform/workers-runtime-quotas.md).
