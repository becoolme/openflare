# CLI reference (placeholder)

A fuller `of` CLI reference will be added by the CLI work. This page
only lists commands that exist on the current tree — the usage string in
`vrt-local/src/bin/of.rs` is the source of truth.

Ops talk to `of serve` over its management unix socket while it is
live; with serve down they open the root directly (the writer lock
serializes against a running serve). Secret values come from stdin only
— never argv.

```text
of init --root <dir> --cell <id> --tenant <id> [--mode capnp|stub]
         [--facade-dist <dir>] [--workerd <path>] [--ingress-listen <addr>]
of serve --root <dir> [--supervise] [--vrt-host-bin <p>] [--vrt-provider-bin <p>]
of deploy --root <dir> <worker_dir>
of deploy --preview --root <dir> <worker_dir>
of deploy [--config-sandbox node|worker] --root <dir> <worker_dir>
of check [--env <name>] [--var K:V]… [--tag <t>] [--config-sandbox node|worker] <worker_dir>
of promote --root <dir> [--worker <name|workerId>] <version_id>
of rollback --root <dir> <worker> <version_id>
of hostnames add --root <dir> <worker> <hostname>
of hostnames rm  --root <dir> <worker> <hostname>
of hostnames ls  --root <dir> <worker>
of secret put --root <dir> <secret_ref>        (value on stdin)
of secret rm  --root <dir> <secret_ref>
of secret ls  --root <dir>
of status  --root <dir>
of versions --root <dir> <worker>
of versions list --root <dir> <worker>
of versions prune --root <dir> [<worker>] [--dry-run] [--max-age-days <n>] [--max-count <n>]
of versions retention --root <dir> [--auto|--no-auto] [--max-age-days <n>] [--max-count <n>]
of renew   --root <dir>
of key     --root <dir>                        (print trusted public key)
of health  --root <dir>                        (mgmt /healthz → exit code)
of probe   --url <http://host:port/path> | --file <path>
of do-snapshot --root <dir> --worker <name> --class <uniqueKey> --tenant <id>
of do-migrate  --root <dir> --worker <name> --class <uniqueKey> --tenant <id> --target-epoch <n>
of worker-id --root <dir> <name|workerId>      (print workerId, one line)
of do-key    --root <dir> <name|workerId> <className>  (print class uniqueKey)
of workflows --root <dir> <name|workerId>      (print declared workflows JSON)
of wfop --root <dir> --worker <name|id> --workflow <name> --op <create|get|action|delete|sendEvent>
         [--instance <id>] [--action <pause|resume|restart|terminate>]
         [--params <json>] [--event-type <t>] [--payload <json>] [--tenant <id>]
```

Two root-level ops are implemented but not in the usage banner:

```text
of metrics       --root <dir>                    (Prometheus text off the mgmt socket)
of queue-metrics --root <dir> (--queue-id <26 chars> | --queue-name <name> [--tenant <id>])
```

Version lifecycle details (`deploy --preview`, `promote`, `versions
list`, preview URLs, and the current MVP limits) are in
[deploy versions](versioning.md). A preview deploy's JSON output (and
`/v1/deploy`, `/v1/versions`) carries `previewUrl` — `null` unless the
host configures `[preview] domain` in `root/config/vrt-host.toml`.

`--config-sandbox=worker` evaluates a `cloudflare.config.*` in a workerd
dynamic worker instead of the default Node loader; it needs
`VRT_WORKERD_BIN` and fails closed without one. See
[config-sandbox-spike.md](config-sandbox-spike.md).
