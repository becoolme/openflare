#!/usr/bin/env bash
# Read-only D1 failover drill check: compares the control-plane writer state
# with an expected epoch. Used by docs/d1-failover-drill.md.
set -euo pipefail

: "${CONTROL_PLANE_URL:?set CONTROL_PLANE_URL (e.g. https://plane.example)}"
: "${CELL_ID:?set CELL_ID}"
: "${ADMIN_TOKEN:?set ADMIN_TOKEN (management API token)}"
: "${DATABASE_ID:?set DATABASE_ID (26 lowercase alphanumerics)}"
EXPECT_EPOCH="${EXPECT_EPOCH:-}"
EXPECT_ADDR="${EXPECT_ADDR:-}"

base="${CONTROL_PLANE_URL%/}"
resp="$(curl -fsS -H "authorization: Bearer ${ADMIN_TOKEN}" \
  "${base}/api/v1/cells/${CELL_ID}/d1/databases/${DATABASE_ID}")"

if command -v jq >/dev/null 2>&1; then
  epoch="$(jq -r '.data.database.writer_epoch // 1' <<<"${resp}")"
  addr="$(jq -r '.data.database.writer_addr // ""' <<<"${resp}")"
  lsn="$(jq -r '.data.database.writer_lsn // ""' <<<"${resp}")"
else
  echo "jq is required for this drill check" >&2
  exit 2
fi

echo "database=${DATABASE_ID} writer_epoch=${epoch} writer_addr=${addr:-<static>} writer_lsn=${lsn:-<none>}"

if [[ -n "${EXPECT_EPOCH}" && "${epoch}" != "${EXPECT_EPOCH}" ]]; then
  echo "expected writer_epoch=${EXPECT_EPOCH}, got ${epoch}" >&2
  exit 1
fi
if [[ -n "${EXPECT_ADDR}" && "${addr}" != "${EXPECT_ADDR}" ]]; then
  echo "expected writer_addr=${EXPECT_ADDR}, got ${addr:-<missing>}" >&2
  exit 1
fi
