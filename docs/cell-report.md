# Cell report: metrics and resource stats

The cell controller periodically collects runtime facts and pushes one report
to the control plane. The report is the source of the console's request/error,
latency, CPU, cold-start, D1, queue, Workflow and Durable Object views — the
control plane never fabricates a missing number.

## Cadence and configuration

`[report]` in `cell-controller.toml`:

```toml
[report]
enabled = true          # default: on when [[pods]] or [d1sql] exist
interval_secs = 30      # 5..=3600
timeout_secs = 10       # 1..=60, per HTTP call
```

Each tick:

1. Scrapes every assigned pod's loopback `GET /metrics` (the `vrt-host`
   Prometheus exposition).
2. Diffs counters against the previous scrape and sends **window deltas**.
   A newly seen series after the first sample contributes `0` (unknown
   baseline), never its lifetime value; a process restart sets `reset`.
3. Queries `vrt-sql` `POST /internal/stats` (signed service auth) for the D1
   databases and queues declared by the signed desired.
4. Calls each pod's `POST /v1/introspect/instances` for the declared
   Workflow/DO class dirs and lists `<hex>.sqlite` instance files.
5. `POST /api/internal/clusters/<cell>/vrt/metrics` with one document.

A failed pod/data-plane/class scan only adds an entry to `resources.errors`;
the rest of the report is still pushed. Failed pushes keep the delta state, so
the next successful push covers the lost window.

## Wire shape

`fixtures/vrt-wire/cell-report.json` is the canonical example (the control
plane validates it with `apps/plane/src/cellReport.ts`).

- `pods[]`: `workers[]` (requests/errors/bytes/inflight/duration histogram),
  `tenants[]` (CPU seconds, RSS, DO object/storage gauges), `launches`,
  `wakes`, `wakeSeconds`.
- `resources.d1[id]`: size/pages/tables/indexes from the data plane.
- `resources.queues[id]`: backlog/bytes/visible/in-flight/delayed/dead.
- `resources.workflows[workerId][name]` and
  `resources.durableObjects[workerId][className]`: instance ids/bytes,
  count, storage bytes, truncation.

R2 bucket stats are **not** in the desired (the physical bucket id is
control-plane knowledge), so the control plane requests them on demand with a
read-only `R2_STATS` cell op; the cell returns a bounded list-scan aggregate.

## Collection bounds

- Pod `/metrics` body: 4 MiB; instance response: 4 MiB; report: 8 MiB.
- 1000 instances per class in the report (the API marks `truncated`).
- 500 D1 databases and 500 queues per stats request.
- R2 stats scans at most 20,000 objects / 100 pages and sets `truncated`.

## What is intentionally still null

- Per-worker CPU/cold starts: the runtime reports CPU per tenant process and
  launches per pod; attributing either to a worker would be fake precision.
- Queue `delivered`: lives in the cell-controller's local registry and is not
  part of the pod scrape yet.
- Workflow instance state counters (running/waiting/completed): the engine has
  no status index; the instance files are listed instead.
- DO `activeInstances`: no liveness index.
