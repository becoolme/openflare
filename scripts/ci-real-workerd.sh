#!/usr/bin/env bash
# ci-real-workerd.sh — run every test gated on VRT_WORKERD_BIN (and,
# when a schema file is supplied, WORKERD_CAPNP) against a real,
# already-verified workerd binary.
#
# This is the same batch the `real-workerd` job in
# .github/workflows/ci.yml runs. The job fetches the pinned binary
# itself (see the workflow comments); this script never downloads
# workerd — you point it at a binary you already verified against
# deploy/docker/workerd.lock:
#
#   scripts/ci-real-workerd.sh /path/to/workerd [/path/to/workerd.capnp]
#
# Env knobs (all optional):
#   CARGO_TARGET_DIR   target dir for builds (default: <repo>/target)
#   VRT_TEST_THREADS   test threads per binary (default: 1 — serial
#                      keeps ~60 workerd spawns off a shared runner)
#   VRT_HOST_UNIX_TESTS  forwarded to m16_ratelimit (default: 1; set
#                      to 0 to leave that test self-skipped)
#
# Test inventory (grep -rln 'VRT_WORKERD_BIN\|WORKERD_CAPNP' **/tests):
#
#   Tier 1 — plain `cargo test`; the tests self-skip when the env var
#   is unset, so the same binaries are harmless without a binary:
#     vrt-host:  cache_smoke cron_smoke d1_smoke preflight_smoke
#                r2_smoke wfops_smoke workerd_entry_smoke workflows_smoke
#     vrt-local: preflight
#     vrt-config: workerd_schema (WORKERD_CAPNP, optional arg $2)
#
#   Tier 2 — `#[ignore = "requires VRT_WORKERD_BIN..."]`; run per-target
#   with `-- --ignored`:
#     vrt-host:  ae_smoke do_w1r4_workerd do_wal_poc isolation_gate_egress
#                isolation_gate_limits kv_r2_contract
#                m13r2_swap_workerd m13r5_swap_unix_workerd
#                m14_tcp_connect_workerd m16_ratelimit
#                m17_hyperdrive_workerd m18_hyperdrive_node_pg
#                runaway_workerd s2z_hibernate_workerd
#                waituntil_workerd workerd_entry_smoke
#     m13r5 + m16 additionally need real unix-socket binds (fine on a
#     normal CI host; restricted sandboxes may deny bind(2) — those
#     tests fail there rather than skip). m17/m18 need a PostgreSQL 16
#     bin dir via VRT_TEST_PG_BIN (and m18 needs node + the repo's
#     esbuild/pg/pg-cloudflare deps); both self-skip with a printed
#     reason when the resources are absent.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORKERD="${1:-}"
CAPNP="${2:-}"
if [ -z "$WORKERD" ]; then
  echo "usage: $0 <path-to-workerd> [path-to-workerd.capnp]" >&2
  exit 2
fi
[ -f "$WORKERD" ] || { echo "workerd binary not found: $WORKERD" >&2; exit 2; }
[ -x "$WORKERD" ] || chmod +x "$WORKERD"
WORKERD="$(cd "$(dirname "$WORKERD")" && pwd)/$(basename "$WORKERD")"
export VRT_WORKERD_BIN="$WORKERD"

if [ -n "$CAPNP" ]; then
  [ -f "$CAPNP" ] || { echo "workerd.capnp not found: $CAPNP" >&2; exit 2; }
  CAPNP="$(cd "$(dirname "$CAPNP")" && pwd)/$(basename "$CAPNP")"
  export WORKERD_CAPNP="$CAPNP"
fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
THREADS="${VRT_TEST_THREADS:-1}"

echo "== workerd under test =="
echo "VRT_WORKERD_BIN=$VRT_WORKERD_BIN"
"$VRT_WORKERD_BIN" --version
[ -n "$CAPNP" ] && echo "WORKERD_CAPNP=$CAPNP"

# d1_smoke locates <target>/debug/vrt-sql itself; build every bin once.
echo "== cargo build --workspace --bins =="
cargo build --locked --workspace --bins

echo "== tier 1: env-gated test binaries (VRT_WORKERD_BIN) =="
cargo test --locked -p vrt-host \
  --test cache_smoke \
  --test cron_smoke \
  --test d1_smoke \
  --test preflight_smoke \
  --test r2_smoke \
  --test wfops_smoke \
  --test workerd_entry_smoke \
  --test workflows_smoke \
  -- --test-threads="$THREADS" --nocapture

cargo test --locked -p vrt-local --test preflight \
  -- --test-threads="$THREADS" --nocapture

if [ -n "$CAPNP" ]; then
  cargo test --locked -p vrt-config --test workerd_schema \
    -- --test-threads="$THREADS" --nocapture
else
  echo "skip: no workerd.capnp arg — workerd_schema tests left skipped"
fi

echo "== tier 2: #[ignore]d test binaries (-- --ignored) =="
# Each binary is invoked on its own: `--ignored` runs only the ignored
# tests in it, and targets that need extra resources (PG16, node)
# self-skip inside their tests.
TIER2=(
  ae_smoke
  do_w1r4_workerd
  do_wal_poc
  isolation_gate_egress
  isolation_gate_limits
  kv_r2_contract
  m13r2_swap_workerd
  m13r5_swap_unix_workerd
  m14_tcp_connect_workerd
  m16_ratelimit
  m17_hyperdrive_workerd
  m18_hyperdrive_node_pg
  runaway_workerd
  s2z_hibernate_workerd
  waituntil_workerd
  workerd_entry_smoke
)
FAILED=()
for t in "${TIER2[@]}"; do
  echo "-- $t"
  # Bound each target: a wedged test must fail with its log, not eat the job.
  if ! env "VRT_HOST_UNIX_TESTS=${VRT_HOST_UNIX_TESTS:-1}" \
    timeout -k 15 "${TARGET_TIMEOUT:-900}" \
    cargo test --locked -p vrt-host --test "$t" \
    -- --ignored --test-threads="$THREADS" --nocapture; then
    FAILED+=("$t")
  fi
done

if [ "${#FAILED[@]}" -gt 0 ]; then
  echo "== FAILED tier-2 targets: ${FAILED[*]} ==" >&2
  exit 1
fi

echo "== all real-workerd tests finished =="
