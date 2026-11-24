# D1 failover drill (real cell)

Read-only checks plus a controlled writer kill. Run in a staging cell with an
opt-in `[d1sql].auto_failover` policy and a prepared follower.

## Preconditions

- Control plane reachable; management token with cell admin scope.
- `DATABASE_ID` bound by a worker on `CELL_ID`.
- The follower promote listener and `[d1sql].promoteEndpoint` are configured.
- `[d1sql].auto_failover` entry has `writerProbe`, `writerAddr`,
  `promoteEndpoint`, a short `failureThreshold` (e.g. 2) and `cooldownSecs`
  (e.g. 60).
- A read query and a write query you can replay manually.

## Drill

1. Record the baseline:
   ```bash
   export CONTROL_PLANE_URL=https://plane.example CELL_ID=cell-a \
     ADMIN_TOKEN=... DATABASE_ID=01j...
   ./scripts/check-d1-writer-state.sh
   ```
2. Stop the writer process/pod (not the follower).
3. Watch:
   - `vrt_cell_d1_auto_total{outcome="probe_failed"}` rises;
   - after threshold, `vrt_cell_d1_auto_total{outcome="promoted"}` is 1;
   - control plane applies `D1_AUTO_PROMOTE` and publishes a new revision.
4. Verify the writer moved:
   ```bash
   EXPECT_EPOCH=<new epoch> EXPECT_ADDR=<new writer> \
     ./scripts/check-d1-writer-state.sh
   ```
5. Replay the read/write queries through the worker binding. Writes must land on
   the new writer; the old writer's next commit must fail with `VRT_FENCED`
   (check its logs).
6. Restart the old writer and confirm it **does not** take writes (the
   manifest CAS rejects its stale epoch).

## Rollback / stop

- Disable the policy (`auto_failover = []`) and restart the cell-controller; no
  further automatic promotions occur.
- To move back, run a manual `PROMOTE` on the former writer only after it has
  caught up; never point `writerAddr` at an epoch the manifest does not
  contain.
- If `promote_failed` rises, keep the old writer resident, inspect
  `vrt_cell_d1_promote_total{outcome="failed"}` and the follower logs, and do
  not retry faster than `cooldown_secs`.

## Alerts

See `deploy/alerts/openflare-runtime.yaml` and `docs/runtime-alerts.md`.
