# `node:fs` — the workerd virtual filesystem

`node:fs` is a workerd-native runtime API, not a binding: nothing is
declared in wrangler beyond the compatibility flags, nothing is wired
to a service or socket, and no platform resource sits behind it. The
module exposes workerd's **in-memory virtual filesystem** (VFS) — it
is not a host disk, and none of its contents survive the lifecycle
described below.

**Status: Drop-in for the VFS semantics workerd defines.** Everything
on this page was verified against the pinned fork binary — see
[Verification](#verification).

## Enablement

```toml
compatibility_date = "2026-01-01"
compatibility_flags = ["nodejs_compat"]
```

The module is present when **all** of the following hold:

- `nodejs_compat` is in `compatibility_flags`;
- the effective compatibility date is **2025-09-15 or later** — the
  enable is implied by date, no extra flag is needed;
- `disable_nodejs_fs_module` is **not** set — the explicit disable
  wins over the date-implied enable.

**Do not assume your deploy lands past the implication date.** The
effective date is resolved per deploy (request field > project
`compatibility_date` > worker record > platform default), and worker
records created without an explicit date default to **2024-01-01** —
*before* the implication point, so `node:fs` stays off. In the
standalone (`of`) path `compatibility_date` is a required wrangler
key, so the date is always the declared one. The robust choice in
either case is to declare the date explicitly:

```toml
compatibility_date = "2026-01-01"   # any date >= 2025-09-15 implies the enable
compatibility_flags = ["nodejs_compat"]
```

Tenants pinned to a compatibility date **before** 2025-09-15 can opt
in explicitly instead:

```toml
compatibility_flags = ["nodejs_compat", "enable_nodejs_fs_module"]
```

Both flag names are ordinary stable compatibility flags — they pass
the control-plane allow-list (`isTenantAllowedCompatFlag`) and the
emit-layer validation (`vrt-config/src/compat_flags.rs`) like any
other stable flag.

When the module is not enabled, `import('node:fs')` and
`import ... from 'node:fs'` fail with `No such module "node:fs"` —
a static import takes the whole worker down at module load, so prefer
top-level imports only when the flags above are part of the deploy.

## Specifiers

Both forms resolve:

```js
import fs from 'node:fs';              // node:-prefixed — always with nodejs_compat
import { readFileSync } from 'fs';     // bare — nodejs_compat at date >= 2024-09-23
```

Bare (un-prefixed) builtin specifiers resolve only at compatibility
dates of **2024-09-23 or later**; the deploy-time bundler enforces the
same boundary and rejects bare specifiers it cannot prove resolvable.
Prefer the `node:` form — it is unambiguous at every date.

## The virtual filesystem layout

The root contains exactly three directories:

```
/
├── bundle/     the uploaded modules — read-only
├── tmp/        writable — scoped to the current request
└── dev/        null, zero, full, random
```

### `/bundle` — uploaded modules, read-only

Every module of the deployed bundle appears as a file under its
module name (`/bundle/main.js` for a module named `main.js`).
`readFileSync`/`read` return the exact uploaded bytes — useful for
templates, SQL files and rule-typed `Text`/`Data` modules. Writes
fail: `operation not permitted` for any create/write inside `/bundle`
or at the root itself.

### `/tmp` — writable, scoped to the request

`/tmp` accepts files, directories (`mkdirSync` recursive) and
symlinks, with one overriding rule:

> **Contents live and die with the requesting IoContext.** A file
> written in one request is not visible in any other request —
> concurrent, subsequent, or otherwise.

Two corollaries verified on the pinned binary:

- **The module-top-level trap.** `fs.writeFileSync('/tmp/init.txt', …)`
  at module scope *succeeds silently* — and the file is **invisible
  in every request handler** (top-level evaluation runs in the
  isolate's own scope, not any request's). Do not initialize `/tmp`
  at module scope expecting handlers to see it; there is no error,
  only absence.
- There is no persistence of any kind: nothing crosses requests,
  restarts, redeploys or replicas. Nothing is shared between worker
  instances.

`/tmp` is in-memory inside the tenant workerd process — large writes
bill against the worker's memory, not against any disk quota.

### `/dev` — device files

`null`, `zero`, `full`, `random`. They behave as character devices,
not sized files: `readFileSync('/dev/zero')` returns zero bytes
because there is no length to read — use the file-descriptor APIs
for real data:

```js
const fd = fs.openSync('/dev/random', 'r');
const buf = Buffer.alloc(16);
fs.readSync(fd, buf, 0, 16, null);   // fills 16 random bytes
fs.closeSync(fd);
```

## API surface

The sync API (`readFileSync`, `writeFileSync`, `readdirSync`,
`statSync`, `mkdirSync`, `symlinkSync`, `openSync`/`readSync`,
`existsSync`, …) and the `fs.promises` surface are present — the
implementation is upstream workerd's; behavior details beyond the
contract asserted here (exact error strings, edge semantics) follow
the pinned workerd release, not a platform shim.

## What this is not

The VFS is a compatibility surface for npm packages and per-request
scratch work. It is **not** a storage product:

- Need durable objects/files → [R2](bindings/r2.md).
- Need key-value state → [KV](bindings/kv.md).
- Need a real POSIX filesystem with persistence → a container app,
  not the Workers runtime.

## Deploy-time interaction

The bundler treats `node:*` specifiers (and resolvable bare builtins)
as external — they stay in the bundle as runtime-resolved imports
rather than being inlined or rewritten, so what workerd resolves at
runtime is exactly the upstream builtin. Non-literal
`import()`/`require()` arguments are rejected at deploy time
regardless of target (the closed-import rule); literal
`import('node:fs')` is fine.

## Verification

Verified 2026-11-28 against the pinned fork binary
(`vrt-20261125-31ed629`, upstream `1.20261123.1`, sha256-pinned per
`deploy/docker/workerd.lock`) on linux/arm64 — the same architecture
as production cells:

| Probe | Result |
| --- | --- |
| `nodejs_compat` @ 2026-01-01, no fs flag | module resolves — enable implied by date |
| `nodejs_compat` @ 2025-01-01 + `enable_nodejs_fs_module` | module resolves — explicit opt-in path works |
| `nodejs_compat` + `disable_nodejs_fs_module` | `No such module "node:fs"` — disable wins |
| no `nodejs_compat` | `No such module "node:fs"` |
| `readdirSync('/')` sorted | exactly `bundle,dev,tmp` |
| `/bundle/main.js` | readable; writes to `/bundle/*` and `/` → `operation not permitted` |
| `/tmp` write + read in one request | round-trips; recursive mkdir + symlink work |
| `/tmp/crossreq.txt` written in request A | **absent** in request B |
| `/tmp/init.txt` written at module top level | write succeeds, **absent** in every request |
| `readSync` on `/dev/random` | fills requested bytes |

Regression coverage: `vrt-host/tests/workerd_entry_smoke.rs` —
`real_workerd_node_fs` (`VRT_WORKERD_BIN`-gated, runs in the external
real-workerd e2e). The test asserts the exact probe payload above —
including both cross-request and top-level invisibility — plus the
two negative modes.
