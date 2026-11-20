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
"${DC[@]}" exec -T vrt sh -c "mkdir -p /tmp/conformance && rm -rf '$remote'"
"${DC[@]}" cp "$CASE_DIR" "vrt:$remote"
"${DC[@]}" exec -T vrt vrt deploy --root /data "$remote"
