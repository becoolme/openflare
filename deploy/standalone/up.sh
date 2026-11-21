#!/usr/bin/env bash
# up.sh — bring the standalone stack up, and on failure print WHY.
#
# Docker Compose's `up --wait` only reports
#   Container vrt-standalone-vrt-sql-1 is unhealthy
# — the actual startup error lives in that container's logs. This
# wrapper runs the same bring-up the conformance gate runs and, when it
# fails, dumps `docker compose ps -a` plus the last 200 log lines of
# every unhealthy or non-zero-exit service, so a broken boot is
# diagnosable from the job log alone.
#
# Usage (same environment as the raw compose command):
#   WORKERD_SOURCE=file WORKERD_SHA256=<hex> deploy/standalone/up.sh
#
# Env knobs:
#   COMPOSE_FILE   base compose file (default: ./docker-compose.yml)
#   COMPOSE_PROJECT compose -p project (default: compose's own name)
#   COMPOSE_EXTRA  extra -f overlays, e.g. "-f docker-compose.stub.yml"
#   LOG_TAIL       per-service log lines to dump on failure (default 200)
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$HERE/docker-compose.yml}"
LOG_TAIL="${LOG_TAIL:-200}"

DC=(docker compose -f "$COMPOSE_FILE")
[ -n "${COMPOSE_PROJECT:-}" ] && DC+=(-p "$COMPOSE_PROJECT")
if [ -n "${COMPOSE_EXTRA:-}" ]; then
  # Word-splitting is intended: this is a list of `-f` overlays.
  read -r -a EXTRA <<<"$COMPOSE_EXTRA"
  DC+=("${EXTRA[@]}")
fi

# `bad_services` prints the services that are unhealthy or exited
# non-zero. `vrt-init` exits 0 by design and must not be reported.
bad_services() {
  "${DC[@]}" ps -a --format '{{.Service}}|{{.Health}}|{{.ExitCode}}' 2>/dev/null \
    | awk -F'|' '$2 == "unhealthy" || ($3 != "" && $3 != "0") { print $1 }'
}

dump_diagnostics() {
  local rc="$1" bad="$2"
  echo "=== standalone stack bring-up FAILED (exit $rc) ===" >&2
  echo "--- docker compose ps -a ---" >&2
  "${DC[@]}" ps -a >&2 || true
  if [ -z "$bad" ]; then
    echo "--- no unhealthy/non-zero service matched; dumping all logs ---" >&2
    "${DC[@]}" logs --no-color --tail "$LOG_TAIL" >&2 || true
    return
  fi
  for svc in $bad; do
    echo "--- logs: $svc (last $LOG_TAIL lines) ---" >&2
    "${DC[@]}" logs --no-color --tail "$LOG_TAIL" "$svc" >&2 || true
  done
}

"${DC[@]}" up --build --wait "$@"
rc=$?
if [ "$rc" -eq 0 ]; then
  exit 0
fi

bad="$(bad_services)"
dump_diagnostics "$rc" "$bad"
exit "$rc"
