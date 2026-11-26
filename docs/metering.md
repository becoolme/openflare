# Usage metering (cell → control plane)

The runtime reports per-worker **usage deltas** to the control plane so a
platform can bill and budget on real traffic. This channel is out-of-band:
it is **not** part of the signed `compute.desired` snapshot and changes to it
do not change the wire contract.

## Where it runs

`cell-controller` scrapes each `[[pods]]` vrt-host's Prometheus text
(`GET {base}/metrics`), computes deltas against the previous scrape, aggregates
per worker, and pushes one batch per pod to
`POST {control-plane}/api/internal/clusters/{cell}/vrt/meter/batch` with the
same scoped cell token as the log shipper. See
[`cell-controller/src/meter.rs`](../cell-controller/src/meter.rs).

| Metric family | Meter field |
| --- | --- |
| `vrt_worker_requests_total` | `requests` (summed across `status_class`) |
| `vrt_worker_request_bytes_total` / `vrt_worker_response_bytes_total` | `requestBytes` / `responseBytes` |
| `vrt_worker_subrequests_total` | `subrequests.<binding_type>` |
| `vrt_worker_cron_invocations_total` | `cronInvocations` |
| `vrt_worker_exceptions_total` | `errors` |

Counters are incremented on the platform-trusted path (host ingress proxy and
provider dispatch); worker code cannot write them.

## Delta semantics

- The runtime sends **deltas**, never cumulative values.
- A sample lower than the previous one is a **counter reset** (vrt-host
  restart): its full value becomes the delta.
- Series whose `worker_id` was folded to `_overflow` by the metrics registry's
  cardinality budget are **skipped** (their baseline still advances) and counted
  in `vrt_cell_meter_overflow_dropped_total`. They are not attributable to a
  real worker.

## Batch contract

```json
{
  "schemaVersion": 1,
  "podId": "vrt-host-…",
  "bootId": "cell-…-pid",
  "seq": 12,
  "windowStart": "2026-11-26T10:00:00.000Z",
  "windowEnd": "2026-11-26T10:00:10.000Z",
  "entries": [
    {
      "workerId": "w-…",
      "requests": 1200,
      "requestBytes": 812344,
      "responseBytes": 9182331,
      "subrequests": { "kv": 330, "r2": 12 },
      "cronInvocations": 0,
      "errors": 3
    }
  ]
}
```

Tenant and version attribution are done **by the control plane** from the
worker row; the cell neither sends nor knows team ids. `cpuMs` is omitted
(per-worker CPU needs a workerd-side export, see limitations).

## Delivery semantics

- **Commit-on-push**: the previous sample and `seq` advance only after the
  control plane accepts the batch. A failed push replays the same
  `(bootId, seq)` on the next tick; the control plane dedupes by that key, so a
  partially delivered tick never double counts.
- One scrape + one push per pod per tick (default 10 s), body caps, no unbounded
  retry queue.
- Scrape or push failures only move counters; the tenant request path is never
  touched.

## Observability

| Metric | Meaning |
| --- | --- |
| `vrt_cell_meter_push_total{outcome}` | `pushed` / `scrape_failed` / `push_failed` |
| `vrt_cell_meter_entries_total` | worker entries accepted by the control plane |
| `vrt_cell_meter_overflow_dropped_total` | samples skipped because of `_overflow` folding |

## Limitations

- No on-disk WAL yet: the delta baseline is in memory, so a cell-controller
  restart can drop at most one scrape window.
- No per-worker CPU: `cpuMs` is omitted unless a workerd export is added.
- Granularity is the scrape interval; two samples for the same worker in one
  tick are summed.

## Tests

```bash
cargo test -p cell-controller --lib meter
```

Covers parsing (labels/escapes), per-worker aggregation, first-scrape full
value, counter resets, failed-push replay, and `_overflow` skipping.
