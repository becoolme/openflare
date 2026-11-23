#!/usr/bin/env bash
# e2e-versioning.sh — REAL-machine acceptance for the deploy-version
# lifecycle on the `deploy/standalone` compose stack.
#
# What this proves (one worker, real workerd, real ingress):
#   * `of deploy` v1 serves v1 on the production hostname;
#   * `of deploy --preview` v2 keeps production on v1 while the signed
#     preview URL (token in the query string) serves v2;
#   * `of promote v2` moves production to v2 and the v2 preview URL
#     follows it (it is the active version now), while an archived
#     version's preview URL stops resolving;
#   * `of rollback` returns production to v1;
#   * `of versions prune --dry-run` writes nothing, and the real prune
#     removes only the retention-expired preview (active + newest
#     preview always survive);
#   * `of versions share` mints a token-gated URL: no token / expired
#     token are 403, the correct token is 200.
#
# Usage (from anywhere):
#   WORKERD_SOURCE=file WORKERD_SHA256=<hex> scripts/e2e-versioning.sh
#   WORKERD_SOURCE=image WORKERD_IMAGE=repo/workerd@sha256:… scripts/e2e-versioning.sh
#
# Knobs (all optional):
#   VRT_PORT=18080       host ingress port (default 8080)
#   VRT_IMAGE_TAG=local  image tag the stack builds/uses
#   COMPOSE_FILE=…       base compose file (default deploy/standalone/docker-compose.yml)
#   KEEP_STACK=1         keep the stack (and its volume) for inspection
#   E2E_PREVIEW_SLEEP=62 seconds to wait for a 60 s share token to expire
#
# A real workerd is required (the stub profile cannot serve the preview
# instances). Preview routing is OFF by default in the shipped host
# config, so this script enables `[preview] domain` + `tokenSecretFile`
# in the freshly initialized `/data/config/vrt-host.toml` BEFORE the
# host starts — no restart of `vrt-host` (which would orphan the `vrt`
# container's shared network namespace).
#
# Prerequisites: bash, docker compose, curl, and node (used only to age
# one stored preview so the retention AGE rule is reachable without
# waiting a week; everything else runs inside the stack).
#
# On failure the script dumps the lifecycle state (versions + active
# pointer + recorded HTTP statuses + service logs). Preview URLs carry a
# bearer token, so every diagnostic line is token-redacted.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

for bin in docker curl node; do
  command -v "$bin" >/dev/null 2>&1 || { echo "missing prerequisite: $bin" >&2; exit 2; }
done
docker compose version >/dev/null 2>&1 || { echo "missing prerequisite: docker compose" >&2; exit 2; }

COMPOSE_FILE="${COMPOSE_FILE:-$ROOT/deploy/standalone/docker-compose.yml}"
PROJECT="vrt-versioning-e2e-$$"
PORT="${VRT_PORT:-8080}"
BASE_URL="http://127.0.0.1:${PORT}"
PREVIEW_DOMAIN="preview.vrt.local"
WORKER="ver-e2e"
PROD_HOST="ver.vrt.local"
WORKER_DIR="/tmp/e2e-versioning"
PREVIEW_SLEEP="${E2E_PREVIEW_SLEEP:-62}"

case "${WORKERD_SOURCE:-}" in
  file)
    : "${WORKERD_SHA256:?WORKERD_SHA256 required when WORKERD_SOURCE=file}"
    ;;
  image)
    : "${WORKERD_IMAGE:?WORKERD_IMAGE required when WORKERD_SOURCE=image}"
    ;;
  stub|"")
    echo "e2e-versioning needs a REAL workerd: set WORKERD_SOURCE=file" >&2
    echo "(with WORKERD_SHA256) or WORKERD_SOURCE=image (with WORKERD_IMAGE)." >&2
    echo "The stub profile cannot serve preview instances." >&2
    exit 2
    ;;
  *)
    echo "unknown WORKERD_SOURCE='${WORKERD_SOURCE}'" >&2
    exit 2
    ;;
esac
export WORKERD_SOURCE WORKERD_SHA256="${WORKERD_SHA256:-}" \
       WORKERD_IMAGE="${WORKERD_IMAGE:-}" VRT_PORT="$PORT"
if [ -n "${VRT_IMAGE_TAG:-}" ]; then
  export VRT_IMAGE_TAG
fi

DC=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE")
TMP="$(mktemp -d)"
FAILED=""
HTTP_LOG=()

# Preview URLs are bearer credentials: never print the token, never
# write it to the failure dump.
redact() {
  sed -E 's/(__vrt_preview_token=)[^&"[:space:]]+/\1<redacted>/g'
}

cleanup() {
  local rc=$?
  if [ -n "$FAILED" ] || [ "$rc" -ne 0 ]; then
    echo "=== e2e-versioning FAILED — lifecycle state follows ===" >&2
    echo "--- recorded HTTP statuses ---" >&2
    printf '%s\n' "${HTTP_LOG[@]+"${HTTP_LOG[@]}"}" | redact >&2 || true
    echo "--- of versions list $WORKER ---" >&2
    "${DC[@]}" exec -T vrt of versions list --root /data "$WORKER" 2>&1 \
      | redact >&2 || echo "(versions unavailable)" >&2
    echo "--- of status ---" >&2
    "${DC[@]}" exec -T vrt of status --root /data 2>&1 \
      | redact >&2 || echo "(of status unavailable)" >&2
    echo "--- state/retention policy ---" >&2
    "${DC[@]}" exec -T vrt sh -c 'cat /data/state/retention.json 2>/dev/null || echo "(defaults)"' >&2 \
      || true
    for svc in vrt-init vrt vrt-host; do
      echo "--- logs: $svc (last 120 lines) ---" >&2
      "${DC[@]}" logs --no-color --tail 120 "$svc" 2>&1 | redact >&2 || true
    done
  fi
  rm -rf "$TMP"
  if [ -z "${KEEP_STACK:-}" ]; then
    "${DC[@]}" down -v >/dev/null 2>&1 || true
  else
    echo "KEEP_STACK set — leaving project $PROJECT up (docker compose -p $PROJECT down -v to remove)" >&2
  fi
  exit "$rc"
}
trap cleanup EXIT

step() { echo "== $*"; }
fail() {
  local msg="$1"
  shift || true
  echo "FAILED: $msg" >&2
  if [ "$#" -gt 0 ]; then
    printf '  %s\n' "$@" | redact >&2
  fi
  FAILED=1
  exit 1
}

# ---- JSON helpers (the image ships no jq; deploy output is pretty JSON) ----
json_str() { # field json
  printf '%s\n' "$2" \
    | sed -n "s/^[[:space:]]*\"$1\": \"\([^\"]*\)\".*/\1/p" \
    | head -1
}
json_bool() { # field json
  printf '%s\n' "$2" \
    | sed -nE "s/^[[:space:]]*\"$1\": (true|false).*/\1/p" \
    | head -1
}
json_num() { # field json
  printf '%s\n' "$2" \
    | sed -nE "s/^[[:space:]]*\"$1\": ([0-9]+).*/\1/p" \
    | head -1
}
version_status() { # versionId json
  printf '%s\n' "$2" | awk -v id="$1" '
    /"versionId":/ { cur = $2; gsub(/[",]/, "", cur) }
    /"status":/ && cur == id { s = $2; gsub(/[",]/, "", s); print s; exit }
  '
}

# ---- HTTP helpers -------------------------------------------------------
http_get() { # host path bodyfile [curl args...] -> status code
  local host="$1" path="$2" body="$3"
  shift 3
  curl -sS -o "$body" -D "$body.headers" -w '%{http_code}' --max-time 5 \
    "$@" -H "Host: $host" "$BASE_URL$path" 2>/dev/null || true
}
header_value() { # headersfile header-name
  tr -d '\r' <"$1" | awk -v name="$(printf '%s' "$2" | tr 'A-Z' 'a-z')" \
    'tolower($1) == name":" { print $2; exit }'
}
record() { # label host path code [detail]
  HTTP_LOG+=("$1 host=$2 path=$3 -> ${4:-?}${5:+ ($5)}")
}

wait_prod_body() { # expected budget
  local expected="$1" budget="${2:-60}" code i
  for i in $(seq 1 "$budget"); do
    code="$(http_get "$PROD_HOST" / "$TMP/prod.body")"
    if [ "$code" = "200" ] && [ "$(cat "$TMP/prod.body")" = "$expected" ]; then
      record "prod" "$PROD_HOST" / "$code" "body=$expected"
      return 0
    fi
    sleep 1
  done
  record "prod" "$PROD_HOST" / "${code:-?}" "wanted body=$expected"
  return 1
}

# Split a signed preview URL into the Host header and the token query.
# Never echoes the token.
preview_split() { # url
  local rest host query
  rest="${1#https://}"
  host="${rest%%/*}"
  case "$rest" in
    *\?*) query="${rest#*\?}" ;;
    *) return 1 ;;
  esac
  [ -n "$host" ] && [ -n "$query" ] || return 1
  PREVIEW_HOST="$host"
  PREVIEW_QUERY="$query"
}

preview_get() { # url bodyfile -> status code
  preview_split "$1" || { echo ""; return 1; }
  http_get "$PREVIEW_HOST" "/?$PREVIEW_QUERY" "$2"
}

wait_preview_body() { # url expected-version budget bodyfile
  local url="$1" want="$2" budget="${3:-60}" body="$4" code i ver
  local rest="${url#https://}"
  local host="${rest%%/*}"
  for i in $(seq 1 "$budget"); do
    code="$(preview_get "$url" "$body")"
    ver="$(header_value "$body.headers" x-vrt-preview-version)"
    if [ "$code" = "200" ] && [ "$ver" = "$want" ]; then
      record "preview" "$host" / "$code" "x-vrt-preview-version=$ver"
      return 0
    fi
    sleep 1
  done
  record "preview" "$host" / "${code:-?}" "wanted x-vrt-preview-version=$want"
  return 1
}

wait_preview_code() { # url expected-code budget bodyfile
  local url="$1" want="$2" budget="${3:-30}" body="$4" code i
  local rest="${url#https://}"
  local host="${rest%%/*}"
  for i in $(seq 1 "$budget"); do
    code="$(preview_get "$url" "$body")"
    if [ "$code" = "$want" ]; then
      record "preview" "$host" / "$code"
      return 0
    fi
    sleep 1
  done
  record "preview" "$host" / "${code:-?}" "wanted status=$want"
  return 1
}

sha_state() {
  "${DC[@]}" exec -T vrt sha256sum /data/state/state.json 2>/dev/null | awk '{print $1}'
}
oplog_lines() {
  "${DC[@]}" exec -T vrt sh -c 'wc -l < /data/state/oplog.jsonl' 2>/dev/null | tr -d ' '
}

# ---- worker fixtures ----------------------------------------------------
stage_workers() {
  local v dir
  for v in 1 2 3 4 5; do
    dir="$TMP/worker-v$v"
    mkdir -p "$dir"
    cat >"$dir/vrt.toml" <<EOF
name = "$WORKER"
mainModule = "main.js"
compatibilityDate = "2024-09-25"
hostnames = ["$PROD_HOST"]
EOF
    printf 'export default { fetch() { return new Response("v%s") } }\n' "$v" >"$dir/main.js"
  done
  chmod -R a+rX "$TMP"/worker-v*
  "${DC[@]}" exec -T -u 0 vrt sh -c "rm -rf '$WORKER_DIR' && mkdir -p '$WORKER_DIR'"
  for v in 1 2 3 4 5; do
    "${DC[@]}" cp "$TMP/worker-v$v" "vrt:$WORKER_DIR/"
  done
}

# Age one stored preview so the retention AGE rule is reachable without
# waiting a week. Mirrors `vrt-local/tests/retention.rs`: only
# `createdAtUnixMs` changes; the state document stays otherwise intact.
age_preview() { # versionId
  local id="$1"
  "${DC[@]}" exec -T vrt sh -c 'cat /data/state/state.json' >"$TMP/state.json" \
    || fail "could not read state.json for aging"
  node - "$TMP/state.json" "$id" <<'NODE'
const fs = require("node:fs")
const [file, id] = process.argv.slice(2)
const doc = JSON.parse(fs.readFileSync(file, "utf8"))
let hits = 0
for (const worker of Object.values(doc.workers ?? {})) {
  for (const version of worker.versions ?? []) {
    if (version.versionId === id) {
      version.createdAtUnixMs = 1_000_000_000_000
      hits += 1
    }
  }
}
if (hits !== 1) throw new Error(`expected exactly one stored version ${id}, found ${hits}`)
fs.writeFileSync(file, JSON.stringify(doc, null, 2))
NODE
  # Rewrite the existing inode as the service uid so ownership/mode
  # (0600) are preserved — no state-lock holder is active at this point.
  "${DC[@]}" exec -T vrt sh -c 'cat > /data/state/state.json' <"$TMP/state.json" \
    || fail "could not write aged state.json"
}

# ---- 1. init the root, enable previews, then start the stack ------------
#
# `of init` treats any hand-edit of `config/vrt-host.toml` as drift and
# refuses to re-initialize, so the shipped compose `vrt-init` service
# cannot run a second time after we fill in the `[preview]` table.
# Bring-up is therefore: init once (same service image/command as
# compose), fill the preview table in the generated config, then start
# the long-running services without re-running the one-shot init. The
# services, image, volume and config paths are exactly the compose
# stack's.
step "compose run vrt-init (project $PROJECT)"
"${DC[@]}" run --rm --no-deps -T vrt-init

step "enable [preview] in /data/config/vrt-host.toml (before vrt-host starts)"
# The generated config already ends with a `[preview]` table whose two
# keys are commented out (previews are OFF by default), so a second
# `[preview]` header would be a duplicate-table TOML error. Fill the
# existing table instead.
#
# A second `compose run` would make Compose rebuild the image (the
# service pins `pull_policy: build`), so the volume is patched with a
# plain container built from the SAME image and volume the stack uses.
# Both are read back from Compose itself — no duplicated names.
IMAGE="$("${DC[@]}" config --format json \
  | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>process.stdout.write(JSON.parse(s).services["vrt-init"].image))')"
VOLUME="$(docker volume ls -q \
  --filter "label=com.docker.compose.project=$PROJECT" \
  --filter "label=com.docker.compose.volume=vrt-data" | head -1)"
[ -n "$IMAGE" ] || fail "could not resolve the stack image from compose config"
[ -n "$VOLUME" ] || fail "could not resolve the vrt-data volume for project $PROJECT"
docker run --rm -i --user 10001:10001 -v "$VOLUME:/data" \
  --entrypoint cat "$IMAGE" /data/config/vrt-host.toml >"$TMP/vrt-host.toml" \
  || fail "could not read /data/config/vrt-host.toml"
node - "$TMP/vrt-host.toml" "$PREVIEW_DOMAIN" <<'NODE'
const fs = require("node:fs")
const [file, domain] = process.argv.slice(2)
const text = fs.readFileSync(file, "utf8")
const table = text.match(/^\[preview\][ \t]*$/m)
if (!table) throw new Error("generated vrt-host.toml has no [preview] table")
const at = table.index + table[0].length
const keys = `\ndomain = "${domain}"\ntokenSecretFile = "/data/keys/preview-token.secret"`
fs.writeFileSync(file, text.slice(0, at) + keys + text.slice(at))
NODE
# Rewrite the existing inode as the service uid so ownership and mode
# (0600) are preserved.
docker run --rm -i --user 10001:10001 -v "$VOLUME:/data" \
  --entrypoint sh "$IMAGE" -c 'cat > /data/config/vrt-host.toml' <"$TMP/vrt-host.toml" \
  || fail "could not write /data/config/vrt-host.toml"

step "start vrt-host / vrt-provider / vrt-sql / vrt"
"${DC[@]}" up -d --wait --no-deps vrt-host vrt-provider vrt-sql
# `vrt` shares vrt-host's network namespace, so it starts after the
# host is up (and healthy) — a later forced recreate of vrt-host would
# orphan this netns, which is why the preview config is in place
# before either container is created.
"${DC[@]}" up -d --wait --no-deps vrt

stage_workers

# ---- 2. deploy v1: production serves v1 ---------------------------------
step "deploy v1"
j1="$("${DC[@]}" exec -T vrt of deploy --root /data "$WORKER_DIR/worker-v1")"
V1="$(json_str versionId "$j1")"
[[ "$V1" =~ ^vrt_[0-9a-f]+$ ]] || fail "deploy v1 returned no versionId" "$j1"
echo "v1 versionId=$V1"
wait_prod_body v1 60 || fail "production never served v1"

step "assert v1 is active"
list="$("${DC[@]}" exec -T vrt of versions list --root /data "$WORKER")"
[ "$(json_str activeVersionId "$list")" = "$V1" ] \
  || fail "activeVersionId is not v1" "active=$(json_str activeVersionId "$list")"
[ "$(version_status "$V1" "$list")" = "active" ] \
  || fail "v1 status is not active" "status=$(version_status "$V1" "$list")"

# ---- 3. preview v2: production stays v1, preview URL serves v2 ----------
step "deploy --preview v2"
j2="$("${DC[@]}" exec -T vrt of deploy --root /data --preview "$WORKER_DIR/worker-v2")"
V2="$(json_str versionId "$j2")"
P2_URL="$(json_str previewUrl "$j2")"
[[ "$V2" =~ ^vrt_[0-9a-f]+$ ]] || fail "preview v2 returned no versionId" "$j2"
[ "$V2" != "$V1" ] || fail "preview v2 reused the v1 version id (content did not change)"
case "$P2_URL" in
  https://*) ;;
  *) fail "preview v2 returned no signed previewUrl (is [preview] configured?)" "$j2" ;;
esac
preview_split "$P2_URL" || fail "preview URL is not <host>/?<token>"
echo "v2 versionId=$V2 previewHost=$PREVIEW_HOST"
wait_preview_body "$P2_URL" "$V2" 60 "$TMP/p2.body" \
  || fail "v2 preview URL never served the v2 preview instance"
[ "$(cat "$TMP/p2.body")" = "v2" ] \
  || fail "v2 preview URL served the wrong body" "body=$(cat "$TMP/p2.body")"
[ "$(header_value "$TMP/p2.body.headers" x-vrt-preview)" = "1" ] \
  || fail "preview response is missing x-vrt-preview: 1"
wait_prod_body v1 30 || fail "production moved off v1 after a preview deploy"

step "assert previews require a token"
code="$(http_get "$PREVIEW_HOST" / "$TMP/p2.notoken")"
record "preview-no-token" "$PREVIEW_HOST" / "$code"
[ "$code" = "403" ] || fail "preview host without a token must be 403, got $code"

step "assert v2 is a stored preview while v1 stays active"
list="$("${DC[@]}" exec -T vrt of versions list --root /data "$WORKER")"
[ "$(json_str activeVersionId "$list")" = "$V1" ] || fail "preview deploy moved the active pointer"
[ "$(version_status "$V2" "$list")" = "preview" ] \
  || fail "v2 is not stored as a preview" "status=$(version_status "$V2" "$list")"

# ---- 4. promote v2: production moves, the v2 preview URL follows --------
step "promote v2"
jp="$("${DC[@]}" exec -T vrt of promote --root /data --worker "$WORKER" "$V2")"
[ "$(json_str versionId "$jp")" = "$V2" ] || fail "promote resolved another version" "$jp"
[ "$(json_bool changed "$jp")" = "true" ] || fail "promote did not report changed=true" "$jp"
echo "promote revision=$(json_num revision "$jp")"
wait_prod_body v2 60 || fail "production never moved to v2 after promote"

step "assert the promoted version's preview URL follows it"
wait_preview_body "$P2_URL" "$V2" 30 "$TMP/p2active.body" \
  || fail "v2 preview URL stopped resolving after promote (active version must follow)"
[ "$(cat "$TMP/p2active.body")" = "v2" ] \
  || fail "active version's preview URL served the wrong body" "body=$(cat "$TMP/p2active.body")"

step "assert promote is idempotent"
jp2="$("${DC[@]}" exec -T vrt of promote --root /data --worker "$WORKER" "$V2")"
[ "$(json_bool changed "$jp2")" = "false" ] \
  || fail "re-promoting the active version must report changed=false" "$jp2"

# ---- 5. rollback v1: production returns to v1 ---------------------------
step "rollback v1"
rev="$("${DC[@]}" exec -T vrt of rollback --root /data "$WORKER" "$V1")"
[[ "$rev" =~ ^[0-9]+$ ]] || fail "rollback printed no revision" "$rev"
echo "rollback revision=$rev"
wait_prod_body v1 60 || fail "production did not return to v1 after rollback"

step "assert rolled-back-from version is archived and its preview URL is gone"
list="$("${DC[@]}" exec -T vrt of versions list --root /data "$WORKER")"
[ "$(json_str activeVersionId "$list")" = "$V1" ] || fail "activeVersionId is not v1 after rollback"
[ "$(version_status "$V2" "$list")" = "archived" ] \
  || fail "v2 is not archived after rolling back to v1" "status=$(version_status "$V2" "$list")"
wait_preview_code "$P2_URL" 404 30 "$TMP/p2gone.body" \
  || fail "an archived version's preview host must not resolve (want 404)"

# ---- 6. prune: dry run writes nothing, real prune keeps active+newest ---
step "deploy --preview v3 and v4"
j3="$("${DC[@]}" exec -T vrt of deploy --root /data --preview "$WORKER_DIR/worker-v3")"
V3="$(json_str versionId "$j3")"
j4="$("${DC[@]}" exec -T vrt of deploy --root /data --preview "$WORKER_DIR/worker-v4")"
V4="$(json_str versionId "$j4")"
[[ "$V3" =~ ^vrt_[0-9a-f]+$ ]] || fail "preview v3 returned no versionId" "$j3"
[[ "$V4" =~ ^vrt_[0-9a-f]+$ ]] || fail "preview v4 returned no versionId" "$j4"
[ "$V3" != "$V4" ] || fail "preview v3 and v4 share a version id"
echo "v3 versionId=$V3"
echo "v4 versionId=$V4"

step "age v3 past the retention age limit"
age_preview "$V3"

step "versions prune --dry-run --max-age-days 1 (must write nothing)"
before_sha="$(sha_state)"
before_oplog="$(oplog_lines)"
dry="$("${DC[@]}" exec -T vrt of versions prune --root /data "$WORKER" --dry-run --max-age-days 1)"
[ "$(json_bool dryRun "$dry")" = "true" ] || fail "prune --dry-run did not report dryRun=true" "$dry"
[ "$(json_bool changed "$dry")" = "false" ] || fail "prune --dry-run reported a state change" "$dry"
printf '%s' "$dry" | grep -q "\"versionId\": \"$V3\"" \
  || fail "dry run did not list the aged preview v3" "$dry"
printf '%s' "$dry" | grep -q "\"versionId\": \"$V4\"" \
  && fail "dry run listed v4 although the newest preview must be protected" "$dry"
after_sha="$(sha_state)"
after_oplog="$(oplog_lines)"
[ "$before_sha" = "$after_sha" ] || fail "prune --dry-run modified state.json" "before=$before_sha after=$after_sha"
[ "$before_oplog" = "$after_oplog" ] || fail "prune --dry-run appended to the oplog" "before=$before_oplog after=$after_oplog"
echo "dry run: state.json unchanged ($before_sha), oplog lines unchanged ($before_oplog)"

step "versions prune --max-age-days 1 (real)"
real="$("${DC[@]}" exec -T vrt of versions prune --root /data "$WORKER" --max-age-days 1)"
[ "$(json_bool changed "$real")" = "true" ] || fail "real prune reported no state change" "$real"
printf '%s' "$real" | grep -q "\"versionId\": \"$V3\"" \
  || fail "real prune did not remove the aged preview v3" "$real"
printf '%s' "$real" | grep -q '"reason": "age"' \
  || fail "real prune did not report the age reason" "$real"
printf '%s' "$real" | grep -q "\"versionId\": \"$V1\"" \
  && fail "real prune removed the active version v1" "$real"
printf '%s' "$real" | grep -q "\"versionId\": \"$V4\"" \
  && fail "real prune removed the newest preview v4" "$real"

step "assert prune kept the active version and the newest preview"
list="$("${DC[@]}" exec -T vrt of versions list --root /data "$WORKER")"
printf '%s' "$list" | grep -q "\"versionId\": \"$V3\"" \
  && fail "v3 is still stored after the real prune"
[ "$(json_str activeVersionId "$list")" = "$V1" ] || fail "active pointer moved during prune"
[ "$(version_status "$V1" "$list")" = "active" ] || fail "v1 is no longer active after prune"
[ "$(version_status "$V4" "$list")" = "preview" ] || fail "v4 (newest preview) was not kept"
[ "$(version_status "$V2" "$list")" = "archived" ] || fail "prune touched the archived history v2"

step "deploy --preview v5 and prune --max-count 1"
j5="$("${DC[@]}" exec -T vrt of deploy --root /data --preview "$WORKER_DIR/worker-v5")"
V5="$(json_str versionId "$j5")"
P5_URL="$(json_str previewUrl "$j5")"
[[ "$V5" =~ ^vrt_[0-9a-f]+$ ]] || fail "preview v5 returned no versionId" "$j5"
case "$P5_URL" in
  https://*) ;;
  *) fail "preview v5 returned no signed previewUrl" "$j5" ;;
esac
echo "v5 versionId=$V5"
wait_preview_body "$P5_URL" "$V5" 60 "$TMP/p5.body" \
  || fail "v5 preview URL never served the v5 preview instance"
count_prune="$("${DC[@]}" exec -T vrt of versions prune --root /data "$WORKER" --max-count 1)"
[ "$(json_bool changed "$count_prune")" = "true" ] || fail "count prune reported no state change" "$count_prune"
printf '%s' "$count_prune" | grep -q "\"versionId\": \"$V4\"" \
  || fail "count prune did not remove the older preview v4" "$count_prune"
printf '%s' "$count_prune" | grep -q '"reason": "count"' \
  || fail "count prune did not report the count reason" "$count_prune"
printf '%s' "$count_prune" | grep -q "\"versionId\": \"$V5\"" \
  && fail "count prune removed the newest preview v5" "$count_prune"
list="$("${DC[@]}" exec -T vrt of versions list --root /data "$WORKER")"
[ "$(version_status "$V5" "$list")" = "preview" ] || fail "v5 (newest preview) was not kept"
printf '%s' "$list" | grep -q "\"versionId\": \"$V4\"" \
  && fail "v4 is still stored after the count prune"
[ "$(json_str activeVersionId "$list")" = "$V1" ] || fail "count prune moved the active pointer"

step "assert the kept preview still resolves after the prune reconcile"
wait_preview_body "$P5_URL" "$V5" 30 "$TMP/p5after.body" \
  || fail "v5 preview URL stopped resolving after the count prune"

# ---- 7. share: token-gated access, expiry -------------------------------
step "versions share v5 --ttl-seconds 60"
share="$("${DC[@]}" exec -T vrt of versions share --root /data --worker "$WORKER" --ttl-seconds 60 "$V5")"
S_URL="$(json_str url "$share")"
S_VERSION="$(json_str versionId "$share")"
[ "$S_VERSION" = "$V5" ] || fail "share resolved another version" "$share"
case "$S_URL" in
  https://*) ;;
  *) fail "versions share printed no signed url" "$share" ;;
esac
# `preview_get` runs inside `$(...)`, so split once in this shell: the
# unauthenticated/expired requests below and the recorded host label
# need the same host.
preview_split "$S_URL" || fail "share URL is not <host>/?<token>"
SHARE_HOST="$PREVIEW_HOST"

step "assert the shared token is accepted"
code="$(preview_get "$S_URL" "$TMP/share.body")"
record "share-token" "$SHARE_HOST" / "$code"
[ "$code" = "200" ] || fail "shared token was rejected (got $code)"
[ "$(cat "$TMP/share.body")" = "v5" ] || fail "shared token served the wrong body" "body=$(cat "$TMP/share.body")"
[ "$(header_value "$TMP/share.body.headers" x-vrt-preview-version)" = "$V5" ] \
  || fail "shared token served another version"

step "assert the same host without a token is 403"
code="$(http_get "$SHARE_HOST" / "$TMP/share.notoken")"
record "share-no-token" "$SHARE_HOST" / "$code"
[ "$code" = "403" ] || fail "preview host without a token must be 403, got $code"

step "wait ${PREVIEW_SLEEP}s for the 60 s token to expire"
sleep "$PREVIEW_SLEEP"
code="$(preview_get "$S_URL" "$TMP/share.expired")"
body="$(cat "$TMP/share.expired")"
record "share-token-expired" "$SHARE_HOST" / "$code" "reason=$body"
[ "$code" = "403" ] || fail "an expired token must be 403, got $code"
case "$body" in
  *preview_token_expired*) ;;
  *) fail "expired token was not rejected as preview_token_expired" "body=$body" ;;
esac

step "PASS — version lifecycle covered on the standalone stack"
echo "v1=$V1 v2=$V2 v3=$V3 v4=$V4 v5=$V5"
