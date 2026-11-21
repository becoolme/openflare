# Analytics Engine

`env.<binding>` is a native workerd `AnalyticsEngineDataset` — the worker
calls `writeDataPoint({ indexes, doubles, blobs })` and the datapoint is
ingested by the platform's per-tenant Analytics Engine service, spooled,
and flushed to canonical Parquet files.

**Status: Partial** — ingest + full-fidelity Parquet storage; query via
DuckDB/ClickHouse (see below); no sampling, no `_sample_interval`
semantics, no hosted SQL API yet.

## Wrangler configuration

```toml
[[analytics_engine_datasets]]
binding = "WEATHER"
dataset = "weather"   # optional — defaults to the binding name (CF behavior)
```

Accepted per-entry keys: `binding`, `dataset`, `remote` (`remote` is
accepted metadata; remote semantics are not implemented). Anything else
fails closed (`VRT_CONFIG_UNSUPPORTED_FIELD`,
`analytics_engine_datasets[0].<key>`). `dataset` must match
`[A-Za-z0-9_]{1,256}` — it is emitted as a workerd service name, an
`--external-addr` token and a storage path segment.

## Using it

```js
export default {
  async fetch(req, env) {
    env.WEATHER.writeDataPoint({
      indexes: ["route:/weather"],
      doubles: [req.cf?.latitude ?? 0, req.cf?.longitude ?? 0],
      blobs: [req.headers.get("user-agent") ?? ""],
    });
    return new Response("ok");
  },
};
```

The call is fire-and-forget like Cloudflare's: it resolves `void` and a
failed ingest never throws into tenant code — failures surface as
`vrt_host_ae_errors_total` metrics and the actor `last_error`, not as
silent fake success. A tenant whose AE socket failed to bind gets an
explicitly degraded launch: `writeDataPoint()` then fails inside
workerd rather than pretending to ingest.

## How it works

- Each `analyticsEngine` binding renders to workerd's
  `(name = <binding>, analyticsEngine = "<dataset>")`. workerd copies
  the service-designator name into the event's `dataset` field, so one
  `external` service is emitted per distinct dataset; all of them
  resolve via `--external-addr` to the tenant's single AE unix socket.
- The socket is bound once per tenant actor (like the Cache API socket)
  and chowned to the tenant's workerd uid/gid — cross-tenant writes die
  on EACCES. Tenant identity is the socket; the wire carries no
  authentication and none is needed.
- workerd POSTs one `AnalyticsEngineEvent` JSON body
  (`{accountId, dataset, schemaVersion, index1, timestamp, double1..20,
  blob1..20}` — `Data` fields as byte arrays) to `https://fake-host/`
  over that channel.
- Accepted events append as raw JSON lines to a bounded per-tenant
  spool, then flush to Parquet. Spool overflow answers `503`
  (observable loss, never fake 2xx); a crash leaves `.flushing` claims
  that the next flush recovers.

## Storage layout

```
<work>/ae/spool/<seg(tenant)>/<dataset>.jsonl        # crash-recoverable buffer
<work>/ae/out/<seg(tenant)>/<dataset>/dt=<YYYY-MM-DD>/part-<digest>.parquet
```

Schema: `tenant Utf8`, `dataset Utf8`, `ts Timestamp(ns, UTC)`,
`index1 Utf8?`, `double1..20 Float64?`, `blob1..20 Utf8?`.
Datapoints are stored at full fidelity — there is no sampling and no
`_sample_interval` column (Cloudflare's sampling exists for their
scale; exact storage is simpler and cheaper at ours).

## Querying

Canonical data is engine-neutral Parquet — any tool that reads Parquet
queries it:

```sh
# Standalone: DuckDB over the local part files
duckdb -c "SELECT blob1, avg(double1) AS v
           FROM 'work/ae/out/*/weather/dt=*/part-*.parquet'
           GROUP BY 1 ORDER BY 2 DESC"
```

```sql
-- Platform: ClickHouse table function over synced object-store copies
SELECT blob1, avg(double1) AS v
FROM s3('https://<bucket>/ae/<seg>/<dataset>/dt=*/part-*.parquet', 'Parquet')
GROUP BY 1
```

Hourly/daily rollups, compaction of small part files, a control-plane
SQL proxy with tenant-scoped views, and TTL retention are follow-up
work — the layout is stable so queries written today keep working.

## Limits and differences

- `dataset` charset `[A-Za-z0-9_]{1,256}` (stricter than CF's free-form
  names — the value is also a path segment and service token).
- `remote` accepted but has no effect.
- No sampling/extrapolation: aggregations are exact.
- Retention/compaction/query API are operator-managed in v1.
- The binding is `$experimental`-gated upstream; the platform derives
  `--experimental` automatically when any worker declares one.
