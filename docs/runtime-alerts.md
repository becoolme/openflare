# Runtime alerts and first response

Rules live in `deploy/alerts/openflare-runtime.yaml`; the drill for D1 is
`docs/d1-failover-drill.md`.

## D1 promotion

- **OpenflareD1PromoteFailures / AutoPromoteFailures**: check
  `vrt_cell_d1_promote_total{outcome="failed"}` in the cell-controller log for
  `VRT_PROMOTE_*`, verify the follower is caught up and the object store is
  reachable. Automatic failover stops after `cooldown_secs`; do not force a
  retry sooner.
- **AutoProbeFlapping**: inspect the writer `/healthz` and network path. A
  flapping writer can promote repeatedly; raise `failure_threshold` or disable
  `auto_failover` until the writer is stable.

## DO alarms and gates

- **AlarmWakeFailures / AlarmUnassigned**: a due alarm has no serving pod or the
  pod wake path is failing. Check `vrt_cell_do_alarm_wake_total`,
  `/v1/do-alarm/wake` status and tenant assignment. Unassigned alarms keep the
  class unsafe, so the tenant stays resident (fail safe).
- **AlarmsStuckDue**: the index still has due entries but no successful wake.
  Check the pod control listener, the `WakeSourceKind::Alarm` wake metric and
  the class `_cf_ALARM` scan.
- **DoSessionsFailReattach**: check
  `vrt_host_ws_session_end_total{reason=...}` and ingress logs; a replay
  overflow means the bounded window is too small for that workload, not that
  data was truncated. Increase `hibernationReplayBytes` or keep those tenants
  resident.

## Scale-to-zero / alarm wake latency

- **AlarmWakeSlow**: p95 of `vrt_host_wake_seconds{source="alarm"}` is above
  30 s. Check pod CPU/startup time and `vrt_host_wake_total{source="alarm",
  outcome!="ok"}`. `[scaleToZero].alarmWakeGraceMs` only keeps an already
  Ready tenant awake; raising it does not speed up workerd boot.

## General

- Gates are advisory only until they expire; an expired/missing gate keeps DO
  tenants resident. Alert on the generation of *unsafe* gates only when it
  persists, not on a single sample.
