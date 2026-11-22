#!/usr/bin/env bash
# deploy-standalone.sh — deploy one staged conformance case directory
# into a running standalone compose stack.
#
# Called by conformance/run.mjs --deploy-cmd; the runner exports:
#   CASE_DIR      staged worker dir (manifest + modules, no case.json)
#   CASE_ID       case id
#   CASE_HOSTNAME Host the case will be requested under
#
# The management API deploys server-local paths, so the dir is copied
# into the vrt container first (same flow as deploy/standalone/e2e.sh).
#
# Env knobs:
#   COMPOSE_FILE       base compose file (default: ../deploy/standalone/docker-compose.yml)
#   COMPOSE_PROJECT    compose -p project (default: compose default)
#   COMPOSE_EXTRA      extra -f overlays, e.g. "-f docker-compose.stub.yml"
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$HERE/../deploy/standalone/docker-compose.yml}"
CASE_DIR="${CASE_DIR:?CASE_DIR required (set by run.mjs)}"
CASE_ID="${CASE_ID:?CASE_ID required}"

DC=(docker compose -f "$COMPOSE_FILE")
[ -n "${COMPOSE_PROJECT:-}" ] && DC+=(-p "$COMPOSE_PROJECT")
[ -n "${COMPOSE_EXTRA:-}" ] && DC+=($COMPOSE_EXTRA)

remote="/tmp/conformance/$CASE_ID"

# Fail fast with evidence when the stack is not healthy. `docker compose
# up --wait` only reports "container ... is unhealthy"; without this the
# runner would time out with no diagnostics. `vrt-init` exits 0 by
# design, so only unhealthy/non-zero services are reported.
bad="$("${DC[@]}" ps -a --format '{{.Service}}|{{.Health}}|{{.ExitCode}}' 2>/dev/null \
  | awk -F'|' '$2 == "unhealthy" || ($3 != "" && $3 != "0") { print $1 }' || true)"
if [ -n "$bad" ]; then
  echo "deploy: standalone stack is not healthy: $bad" >&2
  "${DC[@]}" ps -a >&2 || true
  for svc in $bad; do
    echo "--- logs: $svc (last 200 lines) ---" >&2
    "${DC[@]}" logs --no-color --tail 200 "$svc" >&2 || true
  done
  exit 1
fi

# A previous deploy may have left root-owned files (compose cp extracts
# as root), so clean up as root: uid 10001 cannot unlink them under the
# sticky /tmp.
"${DC[@]}" exec -T -u 0 vrt sh -c "mkdir -p /tmp/conformance && rm -rf '$remote'"
# The runner stages the case in a mkdtemp dir (0700). `docker compose cp`
# extracts the archive as root inside the container, so the non-root
# service uid could neither read the files nor traverse the dirs. The
# staged tree is public repo fixtures — make it readable before copying.
chmod -R a+rX "$CASE_DIR"
"${DC[@]}" cp "$CASE_DIR" "vrt:$remote"
"${DC[@]}" exec -T vrt of deploy --root /data "$remote"
