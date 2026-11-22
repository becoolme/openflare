#!/usr/bin/env bash
# T25/D — standalone REAL-machine acceptance (run outside the sandbox).
#
# Proves the base compose path end-to-end with a REAL workerd:
#   up --wait → init → deploy hello + kv-assets → Host-header curls
#   (200/body), assets ETag/304 + SPA fallback, of status, restart
#   recovery. Any failure dumps every service's logs; the project is
#   always torn down (down -v) — the next run starts clean.
#
# Usage (from this directory or anywhere):
#   WORKERD_SOURCE=file WORKERD_SHA256=<hex> ./e2e.sh
#   WORKERD_SOURCE=image WORKERD_IMAGE=repo/workerd@sha256:… ./e2e.sh
#   VRT_PORT=18080 ./e2e.sh        # optional host port (default 8080)
#
# Never: sudo, unpinned image pulls, token printing.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

COMPOSE_FILE="docker-compose.yml"
PROJECT="vrt-b8-e2e-$$"
PORT="${VRT_PORT:-8080}"
BASE_URL="http://127.0.0.1:${PORT}"

# Fail closed, early: WORKERD_SOURCE must be an explicit real source.
case "${WORKERD_SOURCE:-}" in
  file)
    : "${WORKERD_SHA256:?WORKERD_SHA256 required when WORKERD_SOURCE=file}"
    ;;
  image)
    : "${WORKERD_IMAGE:?WORKERD_IMAGE required when WORKERD_SOURCE=image}"
    ;;
  *)
    echo "e2e requires a REAL workerd: set WORKERD_SOURCE=file (with" >&2
    echo "WORKERD_SHA256) or WORKERD_SOURCE=image (with WORKERD_IMAGE)." >&2
    echo "The stub profile has its own overlay — see README." >&2
    exit 2
    ;;
esac

export WORKERD_SOURCE WORKERD_SHA256="${WORKERD_SHA256:-}" \
       WORKERD_IMAGE="${WORKERD_IMAGE:-}" VRT_PORT="$PORT"

DC=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE")
FAILED=""

cleanup() {
  local rc=$?
  if [ -n "$FAILED" ] || [ "$rc" -ne 0 ]; then
    echo "=== e2e FAILED — service logs follow ===" >&2
    for svc in vrt-init vrt vrt-host vrt-provider vrt-sql; do
      echo "--- $svc ---" >&2
      "${DC[@]}" logs --no-color --tail=200 "$svc" >&2 || true
    done
    # T37b-r4: bounded diagnostics — a wedged `docker compose exec`
    # or a hung socket read must not stall the dump (stderr is left
    # UNSUPPRESSED so the actual error is visible; `timeout` wraps
    # each exec where coreutils provides it).
    local TIMEOUT=()
    if command -v timeout >/dev/null 2>&1; then TIMEOUT=(timeout 30); fi
    # T37b-r: queue dispatcher metrics live in the `vrt` process's
    # cell-controller registry — the service logs alone can't show
    # claim/lease/delivery state, so pull them off the mgmt socket.
    echo "--- vrt queue dispatcher metrics ---" >&2
    ${TIMEOUT[@]+"${TIMEOUT[@]}"} "${DC[@]}" exec -T vrt of metrics --root /data \
      | grep -E "vrt_queue_delivered_total|vrt_queue_consumer_errors_total|vrt_queue_dispatcher_lease|vrt_queue_dispatcher_epoch|vrt_queue_settle_blocked_total" \
      >&2 || echo "(dispatcher metrics unavailable)" >&2
    # T37b-r2: generation state is the first thing an operator checks —
    # switch-failed/launch detail must be in the failure dump.
    echo "--- of status (tenant generations) ---" >&2
    ${TIMEOUT[@]+"${TIMEOUT[@]}"} "${DC[@]}" exec -T vrt of status --root /data >&2 \
      || echo "(of status unavailable)" >&2
    # T37b-r4: queue backlog — send-ok-but-no-delivery splits into
    # visible (never claimed) / inFlight (claimed, unsettled) /
    # dead (dropped after retries). Signed GET via the provider's
    # [vrtSql] channel — no extra tooling in the image.
    echo "--- queue backlog (e2e-queue) ---" >&2
    ${TIMEOUT[@]+"${TIMEOUT[@]}"} "${DC[@]}" exec -T vrt \
      of queue-metrics --root /data --queue-name e2e-queue >&2 \
      || echo "(queue metrics unavailable)" >&2
    # T37b-r3: in-container probes run the image's own `vrt` binary —
    # the runtime images ship no curl (do NOT add it for diagnostics).
    echo "--- vrt-host control / provider listener probes ---" >&2
    ${TIMEOUT[@]+"${TIMEOUT[@]}"} "${DC[@]}" exec -T vrt-host of probe --url http://127.0.0.1:9091/healthz >&2 \
      || echo "(vrt-host control probe failed)" >&2
    ${TIMEOUT[@]+"${TIMEOUT[@]}"} "${DC[@]}" exec -T vrt-provider of probe --dir-sockets /data/provider/listeners >&2 \
      || echo "(provider listener probe failed)" >&2
  fi
  "${DC[@]}" down -v >/dev/null 2>&1 || true
  exit "$rc"
}
trap cleanup EXIT

step() { echo "== $*"; }

# 1. Bring the stack up exactly as the README documents — no overlay.
step "compose up -d --wait (project $PROJECT)"
"${DC[@]}" up -d --wait

# 2. Copy the example workers into the shared volume via the vrt
#    service (the management API deploys server-local paths).
step "stage example workers"
EXAMPLES="$(cd "$HERE/../../examples" && pwd)"
"${DC[@]}" exec -T vrt sh -c 'mkdir -p /tmp/examples'
"${DC[@]}" cp "$EXAMPLES/standalone-hello" vrt:/tmp/examples/
"${DC[@]}" cp "$EXAMPLES/standalone-kv-assets" vrt:/tmp/examples/
"${DC[@]}" cp "$EXAMPLES/standalone-wrangler" vrt:/tmp/examples/
"${DC[@]}" cp "$EXAMPLES/standalone-d1" vrt:/tmp/examples/
"${DC[@]}" cp "$EXAMPLES/standalone-queue" vrt:/tmp/examples/
"${DC[@]}" cp "$EXAMPLES/standalone-do" vrt:/tmp/examples/
"${DC[@]}" cp "$EXAMPLES/standalone-workflow" vrt:/tmp/examples/
"${DC[@]}" cp "$EXAMPLES/standalone-r2" vrt:/tmp/examples/
"${DC[@]}" cp "$EXAMPLES/standalone-cache" vrt:/tmp/examples/
"${DC[@]}" cp "$EXAMPLES/standalone-cron" vrt:/tmp/examples/

# The hello worker declares a secretRef — seed it BEFORE deploy so
# the first render resolves (render fails closed on missing secrets).
step "seed greeting-secret"
printf 'e2e-greeting-secret' | "${DC[@]}" exec -T vrt \
  of secret put --root /data greeting-secret >/dev/null

step "deploy standalone-hello"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-hello

step "deploy standalone-kv-assets"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-kv-assets

step "deploy standalone-wrangler"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-wrangler

step "deploy standalone-d1"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-d1

step "deploy standalone-queue (producer+consumer)"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-queue

step "deploy standalone-do"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-do

step "deploy standalone-workflow"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-workflow

step "deploy standalone-r2"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-r2

step "deploy standalone-cache (C1)"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-cache

step "deploy standalone-cron (CR1)"
"${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-cron

# W4-r2 (append-only): the last deploy lands a generation switch; the
# first assertions must target the FINAL generation, not whatever was
# serving mid-switch. `/readyz` is the authoritative check — it reports
# ready only when EVERY assigned worker has state Ready AND
# servingDigest == expectedDigest (open endpoint). Poll via the
# image's own `of probe` (the image ships no curl/jq); on timeout
# dump `of status` so the stuck generation is visible.
step "wait servingDigest == expectedDigest (post-deploy)"
converged=0
for _ in $(seq 1 60); do
  if "${DC[@]}" exec -T vrt-host of probe --url http://127.0.0.1:9091/readyz >/dev/null 2>&1; then
    converged=1
    break
  fi
  sleep 1
done
if [ "$converged" != "1" ]; then
  echo "host never converged to servingDigest == expectedDigest" >&2
  "${DC[@]}" exec -T vrt of status --root /data >&2 || true
  FAILED=1
  exit 1
fi

# 3. Host-header curl assertions. workerd takes ~seconds to converge;
#    poll briefly before asserting.
http_code() { # host-header path
  curl -sS -o /dev/null -w '%{http_code}' --max-time 3 \
    -H "Host: $1" "$BASE_URL$2" || true
}
wait_ready() { # hostname path [budget-secs=30] → wait for 200
  local i budget="${3:-30}"
  for i in $(seq 1 "$budget"); do
    [ "$(http_code "$1" "$2")" = "200" ] && return 0
    sleep 1
  done
  return 1
}

step "assert hello worker"
wait_ready hello.vrt.local / || { echo "hello never became ready" >&2; FAILED=1; exit 1; }
body="$(curl -sS --max-time 5 -H "Host: hello.vrt.local" "$BASE_URL/")"
echo "$body" | grep -q '"greeting":"hello from a var"' \
  || { echo "hello body unexpected: $body" >&2; FAILED=1; exit 1; }
echo "$body" | grep -q '"secretPresent":true' \
  || { echo "secret binding unresolved: $body" >&2; FAILED=1; exit 1; }

step "assert KV counter"
wait_ready kv-assets.vrt.local /api/hits || { FAILED=1; exit 1; }
h1="$(curl -sS --max-time 5 -H "Host: kv-assets.vrt.local" "$BASE_URL/api/hits")"
h2="$(curl -sS --max-time 5 -H "Host: kv-assets.vrt.local" "$BASE_URL/api/hits")"
n1="${h1##*:}"; n1="${n1%\}*}"; n2="${h2##*:}"; n2="${n2%\}*}"
[ "${n2:-0}" -gt "${n1:-0}" ] \
  || { echo "KV counter did not increment: $h1 / $h2" >&2; FAILED=1; exit 1; }

step "assert assets ETag + 304"
wait_ready kv-assets.vrt.local /app.js || { FAILED=1; exit 1; }
etag="$(curl -sSI --max-time 5 -H "Host: kv-assets.vrt.local" "$BASE_URL/app.js" \
  | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')"
[ -n "$etag" ] || { echo "no ETag on /app.js" >&2; FAILED=1; exit 1; }
code304="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 \
  -H "Host: kv-assets.vrt.local" -H "If-None-Match: $etag" "$BASE_URL/app.js")"
[ "$code304" = "304" ] \
  || { echo "expected 304, got $code304" >&2; FAILED=1; exit 1; }

step "assert wrangler.jsonc worker (T31)"
wait_ready wrangler.vrt.local / || { echo "wrangler worker never ready" >&2; FAILED=1; exit 1; }
body="$(curl -sS --max-time 5 -H "Host: wrangler.vrt.local" "$BASE_URL/")"
echo "$body" | grep -q '"greeting":"hello from wrangler.jsonc"' \
  || { echo "wrangler vars unresolved: $body" >&2; FAILED=1; exit 1; }
w1="$(curl -sS --max-time 5 -H "Host: wrangler.vrt.local" "$BASE_URL/api/hits")"
w2="$(curl -sS --max-time 5 -H "Host: wrangler.vrt.local" "$BASE_URL/api/hits")"
wn1="${w1##*:}"; wn1="${wn1%\}*}"; wn2="${w2##*:}"; wn2="${wn2%\}*}"
[ "${wn2:-0}" -gt "${wn1:-0}" ] \
  || { echo "wrangler KV (derived id) did not increment: $w1 / $w2" >&2; FAILED=1; exit 1; }
wait_ready wrangler.vrt.local /app.js || { FAILED=1; exit 1; }
curl -sS --max-time 5 -H "Host: wrangler.vrt.local" "$BASE_URL/app.js" \
  | grep -q "wrangler static asset" \
  || { echo "wrangler assets not served" >&2; FAILED=1; exit 1; }

step "assert D1 counter (T34)"
wait_ready d1.vrt.local /api/hits || {
  echo "d1 worker never ready; /api/hits body:" >&2
  curl -sS --max-time 5 -H "Host: d1.vrt.local" "$BASE_URL/api/hits" >&2 || true
  FAILED=1; exit 1;
}
d1="$(curl -sS --max-time 5 -H "Host: d1.vrt.local" "$BASE_URL/api/hits")"
d2="$(curl -sS --max-time 5 -H "Host: d1.vrt.local" "$BASE_URL/api/hits")"
dn1="${d1##*:}"; dn1="${dn1%\}*}"; dn2="${d2##*:}"; dn2="${dn2%\}*}"
[ "${dn2:-0}" -gt "${dn1:-0}" ] \
  || { echo "D1 counter did not increment: $d1 / $d2" >&2
       echo "third fetch body:" >&2
       curl -sS --max-time 5 -H "Host: d1.vrt.local" "$BASE_URL/api/hits" >&2 || true
       FAILED=1; exit 1; }

# T36b: restart ONLY vrt-sql -- fs persistence + graceful lease release should let
# the counter continue immediately (SIGTERM releases the lease; a new holder need not wait the full 15s).
step "restart vrt-sql only (persistence + lease release)"
"${DC[@]}" restart vrt-sql
wait_ready d1.vrt.local /api/hits \
  || { echo "d1 worker did not recover after vrt-sql restart" >&2; FAILED=1; exit 1; }
ds="$(curl -sS --max-time 5 -H "Host: d1.vrt.local" "$BASE_URL/api/hits")"
dns="${ds##*:}"; dns="${dns%\}*}"
[ "${dns:-0}" -gt "${dn2:-0}" ] \
  || { echo "D1 data did not survive vrt-sql restart: $ds" >&2; FAILED=1; exit 1; }
dn2="$dns"

# T37b: producer send → standalone dispatcher claims → consumer
# queue() handler writes D1. At-least-once ⇒ assert hits >= sends.
step "assert queue consumer delivery (T37b)"
wait_ready queue.vrt.local /api/hits || {
  echo "queue worker never ready; /api/hits body:" >&2
  curl -sS --max-time 5 -H "Host: queue.vrt.local" "$BASE_URL/api/hits" >&2 || true
  FAILED=1; exit 1;
}
for i in 1 2 3; do
  curl -sS --max-time 5 -X POST -H "Host: queue.vrt.local" \
    "$BASE_URL/send" | grep -q '"ok":true' \
    || { echo "queue send $i failed" >&2; FAILED=1; exit 1; }
done
qhits=""
for i in $(seq 1 30); do
  qb="$(curl -sS --max-time 5 -H "Host: queue.vrt.local" "$BASE_URL/api/hits")"
  qhits="${qb##*:}"; qhits="${qhits%\}*}"
  [ "${qhits:-0}" -ge 3 ] && break
  sleep 1
done
[ "${qhits:-0}" -ge 3 ] \
  || { echo "queue consumer delivered ${qhits:-0} < 3 sends" >&2; FAILED=1; exit 1; }

step "assert DO counter (P3c)"
wait_ready do.vrt.local /get || {
  echo "do worker never ready" >&2; FAILED=1; exit 1;
}
d1="$(curl -sS --max-time 5 -H "Host: do.vrt.local" "$BASE_URL/inc")"
d2="$(curl -sS --max-time 5 -H "Host: do.vrt.local" "$BASE_URL/inc")"
echo "$d2" | grep -q '"n":2'   || { echo "DO counter did not increment: $d1 $d2" >&2; FAILED=1; exit 1; }

step "assert DO alarm (P3c)"
curl -sS --max-time 5 -H "Host: do.vrt.local" "$BASE_URL/alarm?ms=800"   | grep -q '"ok":true' || { echo "DO setAlarm failed" >&2; FAILED=1; exit 1; }
af=""
for i in $(seq 1 15); do
  ab="$(curl -sS --max-time 5 -H "Host: do.vrt.local" "$BASE_URL/alarm-fired")"
  af="${ab##*:}"; af="${af%\}*}"
  [ "${af:-0}" -ge 1 ] && break
  sleep 1
done
[ "${af:-0}" -ge 1 ]   || { echo "DO alarm never fired" >&2; FAILED=1; exit 1; }

# Arm an alarm, then restart vrt-host mid-flight — the armed alarm is
# persisted in metadata.sqlite and must still fire after restart.
curl -sS --max-time 5 -H "Host: do.vrt.local" "$BASE_URL/alarm?ms=1500" >/dev/null
step "restart vrt (DO persistence + armed-alarm survival)"
"${DC[@]}" restart vrt
wait_ready do.vrt.local /get 75 || { echo "do worker did not recover after vrt restart" >&2; FAILED=1; exit 1; }
d3="$(curl -sS --max-time 5 -H "Host: do.vrt.local" "$BASE_URL/get")"
echo "$d3" | grep -q '"n":2'   || { echo "DO state did not survive vrt restart: $d3" >&2; FAILED=1; exit 1; }
af2=""
for i in $(seq 1 20); do
  ab2="$(curl -sS --max-time 5 -H "Host: do.vrt.local" "$BASE_URL/alarm-fired")"
  af2="${ab2##*:}"; af2="${af2%\}*}"
  [ "${af2:-0}" -ge 2 ] && break
  sleep 1
done
[ "${af2:-0}" -ge 2 ]   || { echo "armed DO alarm lost across restart (fired=$af2)" >&2; FAILED=1; exit 1; }

step "assert DO snapshot+restore (P3c-2)"
# P3c-2-r2: workerId + uniqueKey come from typed read-only CLI
# queries — never grep `of status` JSON positionally. serde_json
# serializes WorkerStatus keys ALPHABETICALLY, so `versions` sits
# between `name` and `workerId`; the old `"name":"X","workerId":"Y"`
# grep could never match, and `set -e` killed the step silently.
# Every $(…) below is `|| true`-guarded + shape-checked so a failure
# names itself instead of dying on an assignment.
wid="$("${DC[@]}" exec -T vrt of worker-id --root /data do-demo || true)"
echo "$wid" | grep -qE '^w_[0-9a-f]+$' \
  || { echo "of worker-id do-demo returned no workerId: '$wid'" >&2; FAILED=1; exit 1; }
# uniqueKey is server-minted (HMAC) — resolve the declared class name
# ("Counter" in examples/standalone-do/wrangler.toml) via the CLI's
# state lookup; never ls the on-disk dir or grep JSON.
ukey="$("${DC[@]}" exec -T vrt of do-key --root /data "$wid" Counter || true)"
echo "$ukey" | grep -qE '^[0-9a-f]{64}$' \
  || { echo "of do-key $wid Counter returned no uniqueKey: '$ukey'" >&2; FAILED=1; exit 1; }
# On-demand epoch-fenced snapshot via the standalone admin CLI → the
# loopback control endpoint (same trust model as cron/queue). The
# admin resolves the class dir from the desired DO view, so the
# request carries the REAL workerId.
"${DC[@]}" exec -T vrt of do-snapshot --root /data \
  --worker "$wid" --class "$ukey" --tenant local \
  || { echo "do-snapshot admin call failed" >&2; FAILED=1; exit 1; }
# Wipe the local class dir (debian-slim image ships coreutils `rm`;
# plain exec — no shell needed), restart vrt-host — clean SIGTERM
# fences the lease; the boot re-acquires epoch+1, sees the committed
# snapshot ref on an empty dir, and restores it verified.
"${DC[@]}" exec -T vrt-host rm -rf "/data/do/local/$wid/$ukey" \
  || { echo "failed to wipe DO class dir" >&2; FAILED=1; exit 1; }
"${DC[@]}" restart vrt-host >/dev/null \
  || { echo "vrt-host restart failed" >&2; FAILED=1; exit 1; }
"${DC[@]}" restart vrt >/dev/null \
  || { echo "vrt restart failed" >&2; FAILED=1; exit 1; }
wait_ready do.vrt.local /get 75 \
  || { echo "do worker did not recover after snapshot+restore" >&2; FAILED=1; exit 1; }
d4="$(curl -sS --max-time 5 -H "Host: do.vrt.local" "$BASE_URL/get" || true)"
echo "$d4" | grep -q '"n":2' \
  || { echo "DO snapshot+restore lost the counter: '$d4'" >&2; FAILED=1; exit 1; }

step "assert Workflow (W1)"
# Engine = DO inside tenant workerd; env.WF → wf-engine-* service.
# Every $(…) is `|| true`-guarded + shape-checked (r2 pattern).
wid_wf="$("${DC[@]}" exec -T vrt of worker-id --root /data wf-demo || true)"
echo "$wid_wf" | grep -qE '^w_[0-9a-f]+$' \
  || { echo "of worker-id wf-demo returned no workerId: '$wid_wf'" >&2; FAILED=1; exit 1; }
wf_list="$("${DC[@]}" exec -T vrt of workflows --root /data wf-demo || true)"
echo "$wf_list" | grep -q '"name": "order-flow"' \
  || { echo "of workflows wf-demo missing order-flow: '$wf_list'" >&2; FAILED=1; exit 1; }
cid="$(curl -sS --max-time 5 -H "Host: wf.vrt.local" "$BASE_URL/create" || true)"
inst="$(echo "$cid" | grep -o '"id":"[^"]*"' | cut -d'"' -f4 || true)"
[ -n "$inst" ] \
  || { echo "workflow create returned no id: '$cid'" >&2; FAILED=1; exit 1; }
# Restart mid-sleep: the Engine's persisted state + alarm on localDisk
# resumes the instance (P3c placement/fencing covers the Engine class).
"${DC[@]}" restart vrt-host >/dev/null \
  || { echo "vrt-host restart failed (workflow mid-run)" >&2; FAILED=1; exit 1; }
"${DC[@]}" restart vrt >/dev/null \
  || { echo "vrt restart failed (workflow mid-run)" >&2; FAILED=1; exit 1; }
wait_ready wf.vrt.local /ping 75 \
  || { echo "wf worker did not recover after restart" >&2; FAILED=1; exit 1; }
wf_done=""
wf_last=""
# Budget = lease TTL fencing fallback (≈35s) + engine resume + the
# in-flight 2s sleep, with margin — assertion semantics unchanged.
for i in $(seq 1 90); do
  s="$(curl -sS --max-time 5 -H "Host: wf.vrt.local" "$BASE_URL/status?id=$inst" || true)"
  wf_last="$s"
  st="$(echo "$s" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || true)"
  [ "$st" = "complete" ] && { wf_done="$s"; break; }
  [ "$st" = "errored" ] \
    && { echo "workflow errored: $s" >&2; FAILED=1; exit 1; }
  sleep 1
done
[ -n "$wf_done" ] \
  || { echo "workflow $inst never completed after restart; last status: '$wf_last'" >&2; FAILED=1; exit 1; }
echo "$wf_done" | grep -q '"output":"a-b-done"' \
  || { echo "workflow output wrong: '$wf_done'" >&2; FAILED=1; exit 1; }
# persistence: a fresh status read still resolves the completed
# instance from the Engine's durable state.
wf_re="$(curl -sS --max-time 5 -H "Host: wf.vrt.local" "$BASE_URL/status?id=$inst" || true)"
echo "$wf_re" | grep -q '"status":"complete"' \
  || { echo "workflow instance not durable: '$wf_re'" >&2; FAILED=1; exit 1; }

step "assert workflow instance ops via /v1/wfops (W3)"
# The platform ops channel: vrt-host /v1/wfops → sock-wfops-<id>
# unix socket → facade-wfops-<id> → engine WorkflowBinding. `of wfop`
# resolves the worker + declaration uniqueKey from LOCAL state and
# derives the facade token — the same contract the control-plane
# cell-op executor drives, so this asserts the trusted channel
# end-to-end (a tenant cannot reach this socket or mint the token).
w3i="e2e-w3-1"
o="$("${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op create --instance "$w3i" || true)"
echo "$o" | grep -q "\"instanceId\":\"$w3i\"" \
  || { echo "wfop create returned no instanceId: '$o'" >&2; FAILED=1; exit 1; }
# pause lands on queued-or-running; poll paused either way.
"${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op action --instance "$w3i" --action pause >/dev/null \
  || { echo "wfop pause failed" >&2; FAILED=1; exit 1; }
st=""
for i in $(seq 1 20); do
  g="$("${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
    --workflow order-flow --op get --instance "$w3i" || true)"
  st="$(echo "$g" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || true)"
  [ "$st" = "paused" ] && break
  [ "$st" = "errored" ] \
    && { echo "wfop instance errored: $g" >&2; FAILED=1; exit 1; }
  sleep 1
done
[ "$st" = "paused" ] \
  || { echo "instance never paused; last: '$st'" >&2; FAILED=1; exit 1; }
"${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op action --instance "$w3i" --action resume >/dev/null \
  || { echo "wfop resume failed" >&2; FAILED=1; exit 1; }
# resume → runs to completion (the 2s sleep inside the run).
w3_done=""
for i in $(seq 1 40); do
  g="$("${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
    --workflow order-flow --op get --instance "$w3i" || true)"
  st="$(echo "$g" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || true)"
  [ "$st" = "complete" ] && { w3_done="$g"; break; }
  [ "$st" = "errored" ] \
    && { echo "wfop instance errored: $g" >&2; FAILED=1; exit 1; }
  sleep 1
done
echo "$w3_done" | grep -q '"output":"a-b-done"' \
  || { echo "resumed instance did not complete: '$w3_done'" >&2; FAILED=1; exit 1; }
# terminate a fresh instance → terminated; get after terminate
# still resolves through the same channel.
w3t="e2e-w3-2"
"${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op create --instance "$w3t" >/dev/null \
  || { echo "wfop create (terminate target) failed" >&2; FAILED=1; exit 1; }
"${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op action --instance "$w3t" --action terminate >/dev/null \
  || { echo "wfop terminate failed" >&2; FAILED=1; exit 1; }
st=""
for i in $(seq 1 20); do
  g="$("${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
    --workflow order-flow --op get --instance "$w3t" || true)"
  st="$(echo "$g" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || true)"
  [ "$st" = "terminated" ] && break
  sleep 1
done
[ "$st" = "terminated" ] \
  || { echo "instance never terminated; last: '$st'" >&2; FAILED=1; exit 1; }
# Negatives: undeclared workflow fails closed at local resolution;
# an invalid action verb is rejected before any dispatch.
"${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow no-such-wf --op get --instance x >/dev/null 2>&1 \
  && { echo "wfop on undeclared workflow must fail" >&2; FAILED=1; exit 1; }
"${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op action --instance "$w3t" --action explode >/dev/null 2>&1 \
  && { echo "wfop with invalid action must fail" >&2; FAILED=1; exit 1; }

# W6-r2: sendEvent delivers into the instance's durable event queue
# (OrderFlow never calls waitForEvent, but the op channel must accept
# and acknowledge the delivery), and delete commits the instance's
# logical removal — the engine's `deleteAll()` inside the actor's
# storage transaction — such that a later get through the same
# channel fails closed. "deleted" means the logical delete is
# CONFIRMED (a fresh actor reads no INSTANCE_METADATA); the sqlite
# file itself is reclaimed later by the host at a verified no-actor
# safe point, never while a resident actor could hold it.
w3e="e2e-w3-3"
"${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op create --instance "$w3e" >/dev/null \
  || { echo "wfop create (event target) failed" >&2; FAILED=1; exit 1; }
ev="$("${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op sendEvent --instance "$w3e" \
  --event-type approve --payload '{"v":1}' || true)"
echo "$ev" | grep -q '"status":"delivered"' \
  || { echo "wfop sendEvent did not deliver: '$ev'" >&2; FAILED=1; exit 1; }
dl="$("${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op delete --instance "$w3e" || true)"
echo "$dl" | grep -q '"status":"deleted"' \
  || { echo "wfop delete did not confirm storage removal: '$dl'" >&2; FAILED=1; exit 1; }
"${DC[@]}" exec -T vrt of wfop --root /data --worker "$wid_wf" \
  --workflow order-flow --op get --instance "$w3e" >/dev/null 2>&1 \
  && { echo "wfop get on deleted instance must fail" >&2; FAILED=1; exit 1; }

step "assert bad workflow class rejected at deploy (W7)"
# A `class_name` that IS exported but is not a WorkflowEntrypoint
# subclass passes workerd's load probe (verified: entrypoint map only
# requires class/function/ExportedHandler) and dies at engine init —
# the pack-time deploy gate must reject it instead. Deploy MUST fail
# with a clear error and publish nothing.
"${DC[@]}" cp "$EXAMPLES/standalone-workflow-bad-class" vrt:/tmp/examples/ || true
badout="$("${DC[@]}" exec -T vrt of deploy --root /data /tmp/examples/standalone-workflow-bad-class 2>&1 || true)"
echo "$badout" | grep -qiE 'WorkflowEntrypoint|workflow class'   || { echo "bad-class deploy was not rejected with a class error: '$badout'" >&2; FAILED=1; exit 1; }
"${DC[@]}" exec -T vrt of worker-id --root /data wf-bad-class >/dev/null 2>&1   && { echo "bad-class deploy published a worker" >&2; FAILED=1; exit 1; }
# wf-demo keeps serving — a rejected sibling deploy cannot poison it.
ok="$(curl -sS --max-time 5 -H "Host: wf.vrt.local" "$BASE_URL/ping" || true)"
echo "$ok" | grep -qiE 'ok|pong|status'   || { echo "wf-demo stopped serving after bad-class deploy reject: '$ok'" >&2; FAILED=1; exit 1; }

step "assert bad deploy rejected, old keeps serving (W1-r2)"
# A config workerd cannot load must fail `of deploy` BEFORE publish
# — explicit error, no versionId, and the tenant keeps serving the
# last good version (the miniflare:zod outage took every worker down).
"${DC[@]}" exec -T vrt sh -c 'mkdir -p /tmp/badw && printf \
  "name = \"bad-w\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n" \
  > /tmp/badw/vrt.toml && printf \
  "import \"nonexistent:mod\";\nexport default { async fetch() { return new Response(\"x\"); } };\n" \
  > /tmp/badw/main.js'
bad_out="$("${DC[@]}" exec -T vrt of deploy --root /data /tmp/badw 2>&1 || true)"
echo "$bad_out" | grep -q "nonexistent:mod" \
  || { echo "bad deploy must name the load failure: '$bad_out'" >&2; FAILED=1; exit 1; }
case "$bad_out" in *versionId*) echo "bad deploy must not return a versionId: '$bad_out'" >&2; FAILED=1; exit 1;; esac
hello_after="$(curl -sS --max-time 5 -H "Host: hello.vrt.local" "$BASE_URL/" || true)"
echo "$hello_after" | grep -qi "greeting" \
  || { echo "bad deploy disturbed the serving generation: '$hello_after'" >&2; FAILED=1; exit 1; }

step "assert transferred_classes rejected on standalone (DO1)"
# Standalone derives DO uniqueKeys as HMAC(tenant:worker:class) — a
# cross-worker transfer has no storage identity there, so vrt-local
# must refuse the key with an explicit message rather than silently
# dropping or fabricating a lineage (§6.10 standalone tradeoff).
"${DC[@]}" exec -T vrt sh -c 'mkdir -p /tmp/xferw && printf \
  "name = \"xfer-w\"\nmain = \"main.js\"\ncompatibility_date = \"2024-09-25\"\n\n[[migrations]]\ntag = \"v1\"\ntransferred_classes = [{ from = \"A\", from_script = \"src\", to = \"B\" }]\n" \
  > /tmp/xferw/wrangler.toml && printf \
  "export default { async fetch() { return new Response(\"x\"); } };\n" \
  > /tmp/xferw/main.js'
xfer_out="$("${DC[@]}" exec -T vrt of deploy --root /data /tmp/xferw 2>&1 || true)"
echo "$xfer_out" | grep -q "transferred_classes" \
  || { echo "transferred_classes deploy must name the rejected field: '$xfer_out'" >&2; FAILED=1; exit 1; }
case "$xfer_out" in *versionId*) echo "transferred_classes deploy must not return a versionId: '$xfer_out'" >&2; FAILED=1; exit 1;; esac

step "assert SPA fallback"
spa="$(curl -sS --max-time 5 -H "Host: kv-assets.vrt.local" \
  "$BASE_URL/some/deep/route")"
echo "$spa" | grep -q "vrt assets ok" \
  || { echo "SPA fallback did not serve index.html" >&2; FAILED=1; exit 1; }

# W5 (append-only): R2 put/get + multipart chain against the fs
# backend — signed uploadId scope, terminal-state rejection.
step "assert R2 put/get"
wait_ready r2-demo.vrt.local "/get?key=w5-put" || true
put="$(curl -sS --max-time 5 -X POST -d 'w5-payload' -H "Host: r2-demo.vrt.local"   "$BASE_URL/put?key=w5-put")"
echo "$put" | grep -q '"size":10' || { echo "r2 put unexpected: $put" >&2; FAILED=1; exit 1; }
got="$(curl -sS --max-time 5 -H "Host: r2-demo.vrt.local" "$BASE_URL/get?key=w5-put")"
expect_sha="$(printf 'w5-payload' | shasum -a 256 | awk '{print $1}')"
echo "$got" | grep -q '"size":10' || { echo "r2 get size: $got" >&2; FAILED=1; exit 1; }
echo "$got" | grep -q '"sha256":"'"$expect_sha"'"'   || { echo "r2 get sha256: $got vs $expect_sha" >&2; FAILED=1; exit 1; }

step "assert R2 multipart chain"
mp="$(curl -sS --max-time 30 -X POST -H "Host: r2-demo.vrt.local" "$BASE_URL/multipart?key=w5-mp")"
echo "$mp" | grep -q '"expectedSize":10485765'   || { echo "multipart size mismatch: $mp" >&2; FAILED=1; exit 1; }
echo "$mp" | grep -q '"etag":"[0-9a-f]*-3"'   || { echo "multipart composite etag missing: $mp" >&2; FAILED=1; exit 1; }
echo "$mp" | grep -q '"completeCustomMetadata":"e2e"' || { echo "complete() lost customMetadata: $mp" >&2; FAILED=1; exit 1; }
echo "$mp" | grep -q '"completeContentType":"application/octet-stream"' || { echo "complete() lost httpMetadata: $mp" >&2; FAILED=1; exit 1; }
sha="$(echo "$mp" | sed -n 's/.*"sha256":"\([0-9a-f]*\)".*/\1/p')"
exp="$(echo "$mp" | sed -n 's/.*"expectedSha256":"\([0-9a-f]*\)".*/\1/p')"
[ -n "$sha" ] && [ "$sha" = "$exp" ]   || { echo "multipart content hash mismatch: $mp" >&2; FAILED=1; exit 1; }

step "assert R2 abort→complete rejected"
ab="$(curl -sS --max-time 10 -X POST -H "Host: r2-demo.vrt.local" "$BASE_URL/abort?key=w5-ab")"
echo "$ab" | grep -q '"completeAfterAbortRejected":true'   || { echo "complete after abort not rejected: $ab" >&2; FAILED=1; exit 1; }

step "assert R2 cross-key uploadId rejected"
ck="$(curl -sS --max-time 10 -H "Host: r2-demo.vrt.local" "$BASE_URL/crosskey?key=w5-ck")"
echo "$ck" | grep -q '"crossKeyRejected":true'   || { echo "cross-key uploadId not rejected: $ck" >&2; FAILED=1; exit 1; }

# C1 (append-only): Cache API lifecycle against the per-tenant
# service — one call runs put → hit → delete → miss inside the
# worker; a named-cache call proves namespace isolation.
step "assert cache put/hit/delete/miss (C1)"
wait_ready cache.vrt.local /flow   || { echo "cache worker never became ready" >&2; FAILED=1; exit 1; }
cf="$(curl -sS --max-time 5 -H "Host: cache.vrt.local" "$BASE_URL/flow")"
echo "$cf" | grep -q '"hit":"F"'   || { echo "cache hit failed: $cf" >&2; FAILED=1; exit 1; }
echo "$cf" | grep -q '"del":true'  || { echo "cache delete failed: $cf" >&2; FAILED=1; exit 1; }
echo "$cf" | grep -q '"after":"miss"' || { echo "post-delete must miss: $cf" >&2; FAILED=1; exit 1; }
cn="$(curl -sS --max-time 5 -H "Host: cache.vrt.local" "$BASE_URL/named")"
echo "$cn" | grep -q '"hit":"N"'   || { echo "named cache hit failed: $cn" >&2; FAILED=1; exit 1; }
echo "$cn" | grep -q '"defaultMiss":true' || { echo "named cache leaked into default: $cn" >&2; FAILED=1; exit 1; }

# CR1 (append-only): a REAL timed cron — `* * * * *` fires on the next
# CronRunner tick (30s cadence, UTC minute match, no backfill), the
# host dispatches via `sock-cron-<id>` → facade → Fetcher.scheduled(),
# and the worker's scheduled() increments a D1 counter. A durable
# side effect, not an HTTP-200 inference. Worst-case latency ≈ tick
# + minute boundary + converge — 150s budget.
step "assert cron scheduled() fired (CR1)"
wait_ready cron.vrt.local /api/hits   || { echo "cron worker never became ready" >&2; FAILED=1; exit 1; }
chits=""; clast=""; cbraw=""
for i in $(seq 1 150); do
  cbraw="$(curl -sS --max-time 5 -H "Host: cron.vrt.local" "$BASE_URL/api/hits" || true)"
  chits="$(printf '%s' "$cbraw" | sed -n 's/.*"hits":\([0-9][0-9]*\).*/\1/p')"
  [ "${chits:-0}" -ge 1 ] && { clast="$cbraw"; break; }
  sleep 1
done
[ "${chits:-0}" -ge 1 ]   || { echo "cron scheduled() never fired within 150s; raw: '$cbraw'" >&2; FAILED=1; exit 1; }
# Assert the controller surface the worker persisted — `last` is
# `${controller.cron}@${controller.scheduledTime}` (main.js:27).
# cron must equal the deployed expression verbatim; scheduledTime is
# a positive epoch-ms ALIGNED to the minute (CF cron semantics).
clastv="$(printf '%s' "$clast" | sed -n 's/.*"last":"\([^"]*\)".*/\1/p')"
ccron="${clastv%@*}"; cms="${clastv##*@}"
[ "$ccron" = '* * * * *' ]                        || { echo "cron field mismatch: '$clast'" >&2; FAILED=1; exit 1; }
printf '%s' "$cms" | grep -qE '^[0-9]+$'          || { echo "scheduledTime not integer ms: '$clast'" >&2; FAILED=1; exit 1; }
[ "$cms" -gt 0 ] && [ $(( cms % 60000 )) -eq 0 ]  || { echo "scheduledTime not minute-aligned: '$clast'" >&2; FAILED=1; exit 1; }

# 4. of status — the operator's view must be coherent.
step "of status"
"${DC[@]}" exec -T vrt of status --root /data

# 5. Restart the whole stack — services must recover (envelope,
#    objects, listeners, sockets all persisted under /data).
step "compose restart"
"${DC[@]}" restart
step "post-restart assertions"
wait_ready kv-assets.vrt.local /api/hits \
  || { echo "kv-assets did not recover after restart" >&2; FAILED=1; exit 1; }
h3="$(curl -sS --max-time 5 -H "Host: kv-assets.vrt.local" "$BASE_URL/api/hits")"
n3="${h3##*:}"; n3="${n3%\}*}"
[ "${n3:-0}" -gt "${n2:-0}" ] \
  || { echo "KV state did not survive restart: $h3" >&2; FAILED=1; exit 1; }
body="$(curl -sS --max-time 5 -H "Host: hello.vrt.local" "$BASE_URL/")"
echo "$body" | grep -q '"secretPresent":true' \
  || { echo "hello broken after restart: $body" >&2; FAILED=1; exit 1; }
wait_ready d1.vrt.local /api/hits \
  || { echo "d1 worker did not recover after restart" >&2; FAILED=1; exit 1; }
d3="$(curl -sS --max-time 5 -H "Host: d1.vrt.local" "$BASE_URL/api/hits")"
dn3="${d3##*:}"; dn3="${dn3%\}*}"
[ "${dn3:-0}" -gt "${dn2:-0}" ] \
  || { echo "D1 data did not survive restart: $d3" >&2; FAILED=1; exit 1; }

step "e2e PASSED (project $PROJECT torn down next)"
