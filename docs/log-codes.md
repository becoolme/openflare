# Platform log codes

When the ingress (`vrt-host`) fails a request on behalf of a worker it emits a
`level = "platform"` log record with a stable `code`. Codes are append-only;
`vrt-host` has a unit test that checks every code in `PlatformCode::ALL` is
listed here.

| code | status | meaning |
| --- | --- | --- |
| `upstream_connect` | 502 | could not connect / handshake with the worker process |
| `upstream_timeout` | 504 | worker did not respond within the request timeout |
| `upstream_error` | 502 | I/O or protocol error while proxying to the worker |
| `worker_unavailable` | 503 | no socket for this worker in the current generation |
| `tenant_unavailable` | 503 | tenant exists but is temporarily not serving |
| `tenant_circuit_open` | 503 | tenant circuit breaker is open |
| `tenant_wake_timeout` | 503 | scale-to-zero: wake exceeded its deadline (sends `Retry-After`) |
| `tenant_wake_failed` | 503 | scale-to-zero: wake start / readiness probe failed (sends `Retry-After`) |
| `wake_capacity` | 503 | scale-to-zero: host wake concurrency / queue / held-request capacity exhausted (sends `Retry-After`) |
| `internal` | 500 | internal ingress failure (caught at the panic boundary) |
| `route_index_rpc_failed` | 503 | static-asset router fails closed: route index RPC failed |
| `route_index_invalid` | 503 | same: route index is invalid |
| `route_index_digest_mismatch` | 503 | same: route index digest mismatch |
| `route_index_timeout` | 503 | same: route index timed out |
| `can_fetch_rpc_failed` | 503 | same: `can_fetch` RPC failed |
| `can_fetch_timeout` | 503 | same: `can_fetch` timed out |
| `logs_rate_limited` | — | tenant exceeded the per-worker log rate limit; records were dropped |

`payload_too_large`, `bad_host` and `unknown_host` are client / pre-routing
errors and do not produce platform events. At most one record per
`(worker, code)` per second is emitted; the number of coalesced events is
attached to the next record.
