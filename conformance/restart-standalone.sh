#!/usr/bin/env bash
# restart-standalone.sh — restart runtime services of a running
# standalone compose stack in place, for a case's restart step.
#
# Called by conformance/run.mjs --restart-cmd; the runner exports:
#   RESTART_SERVICES  ordered, space-separated compose services
#   CASE_ID           the case that asked for the restart
#
# The restart is deliberately narrow: `docker compose restart` keeps the
# containers and every volume (vrt-data, vrt-sql-objects) intact and
# never re-deploys a worker, so the case observes ONLY the effect of a
# fresh runtime process. The runner probes readiness afterwards; this
# script does not sleep and does not wait for health.
#
# Env knobs (same as deploy-standalone.sh):
#   COMPOSE_FILE       base compose file (default: ../deploy/standalone/docker-compose.yml)
#   COMPOSE_PROJECT    compose -p project (default: compose default)
#   COMPOSE_EXTRA      extra -f overlays, e.g. "-f docker-compose.stub.yml"
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$HERE/../deploy/standalone/docker-compose.yml}"
SERVICES="${RESTART_SERVICES:?RESTART_SERVICES required (set by run.mjs)}"

DC=(docker compose -f "$COMPOSE_FILE")
[ -n "${COMPOSE_PROJECT:-}" ] && DC+=(-p "$COMPOSE_PROJECT")
[ -n "${COMPOSE_EXTRA:-}" ] && DC+=($COMPOSE_EXTRA)

read -r -a LIST <<<"$SERVICES"
[ "${#LIST[@]}" -gt 0 ] \
  || { echo "restart: empty RESTART_SERVICES" >&2; exit 2; }

# Preflight the whole list before touching anything: a typo must not
# leave the stack half-restarted (fail closed, all or nothing).
known="$("${DC[@]}" config --services)"
for svc in "${LIST[@]}"; do
  printf '%s\n' "$known" | grep -qx -- "$svc" \
    || { echo "restart: unknown compose service '$svc'" >&2; exit 2; }
done

# Ordered on purpose: vrt joins vrt-host's network namespace, so
# vrt-host must be recreated first and vrt must follow it.
for svc in "${LIST[@]}"; do
  echo "restart: $svc (case ${CASE_ID:-unknown})"
  "${DC[@]}" restart "$svc"
done
