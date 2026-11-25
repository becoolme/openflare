#!/usr/bin/env bash
# Acceptance gate for the standalone monitoring profile: the canonical
# runtime alert rules must parse, vmalert must load every group/rule, and
# Alertmanager must accept the notifier. Runs against throwaway containers
# on loopback; no host services need to be running.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PROJECT="${COMPOSE_PROJECT:-vrt-alerts-e2e-$$}"
DC=(docker compose -f "$HERE/docker-compose.yml" -p "$PROJECT")

export VM_BIND=127.0.0.1 VM_PORT="${VM_PORT:-18428}"
export VMALERT_BIND=127.0.0.1 VMALERT_PORT="${VMALERT_PORT:-18880}"
export ALERTMANAGER_BIND=127.0.0.1 ALERTMANAGER_PORT="${ALERTMANAGER_PORT:-19093}"

cleanup() { "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "== promtool: validate deploy/alerts/openflare-runtime.yaml"
docker run --rm --entrypoint promtool \
  -v "$ROOT/deploy/alerts:/rules:ro" prom/prometheus:v3.2.1 \
  check rules /rules/openflare-runtime.yaml

echo "== bring up VictoriaMetrics + vmalert + Alertmanager"
"${DC[@]}" up --no-deps -d --wait victoria-metrics alertmanager vmalert

wait_url() {
  local url="$1" name="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timed out waiting for $name ($url)" >&2
  return 1
}

wait_url "http://127.0.0.1:${VMALERT_PORT}/-/healthy" vmalert
wait_url "http://127.0.0.1:${ALERTMANAGER_PORT}/-/healthy" alertmanager

echo "== vmalert loaded the canonical rules"
curl -fsS "http://127.0.0.1:${VMALERT_PORT}/api/v1/rules" |
  node -e '
    let raw = ""
    process.stdin.on("data", (c) => (raw += c))
    process.stdin.on("end", () => {
      const body = JSON.parse(raw)
      if (body.status !== "success") throw new Error(`vmalert status ${body.status}`)
      const groups = body.data?.groups ?? []
      const names = groups.map((g) => g.name).sort()
      const rules = groups.flatMap((g) => g.rules ?? [])
      const expected = [
        "openflare-d1-failover",
        "openflare-do-alarms",
        "openflare-scale-to-zero",
      ]
      if (JSON.stringify(names) !== JSON.stringify(expected)) {
        throw new Error(`unexpected rule groups: ${JSON.stringify(names)}`)
      }
      if (rules.length !== 8) throw new Error(`expected 8 rules, got ${rules.length}`)
      const alerts = rules.map((r) => r.name).sort()
      for (const name of [
        "OpenflareD1PromoteFailures",
        "OpenflareD1AutoPromoteFailures",
        "OpenflareD1AutoProbeFlapping",
        "OpenflareDoAlarmWakeFailures",
        "OpenflareDoAlarmUnassigned",
        "OpenflareDoAlarmsStuckDue",
        "OpenflareDoSessionsFailReattach",
        "OpenflareAlarmWakeSlow",
      ]) {
        if (!alerts.includes(name)) throw new Error(`missing rule ${name}`)
      }
      console.log(`OK: ${groups.length} groups, ${rules.length} rules`)
    })
  '

echo "== Alertmanager healthy and reachable by vmalert"
curl -fsS "http://127.0.0.1:${ALERTMANAGER_PORT}/api/v2/status" |
  node -e 'let r="";process.stdin.on("data",c=>r+=c);process.stdin.on("end",()=>{const b=JSON.parse(r);if(!b.versionInfo?.version)throw new Error("no Alertmanager version");console.log("OK: Alertmanager",b.versionInfo.version)})'

echo "OK: runtime alert rules are wired into the standalone monitoring profile"
